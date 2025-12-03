/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
#include "datasystem/worker/hash_ring/hash_ring.h"

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <iterator>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>

#include <google/protobuf/util/message_differencer.h>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_bool(auto_del_dead_node);

namespace datasystem {
namespace worker {
static constexpr int MAX_CANDIDATE_WORKER_NUM = 5;
const HashRing::HashFunction HashRing::hashFunction_ = MurmurHash3_32;

HashRing::HashRing(std::string workerAddr, EtcdStore *etcdStore)
    : workerAddr_(std::move(workerAddr)),
      etcdStore_(etcdStore),
      state_(INIT),
      enableDistributedMaster_(FLAGS_enable_distributed_master)
{
}

HashRing::~HashRing()
{
    exitFlag_ = true;
    LOG(INFO) << "HashRing exit";
}

Status HashRing::InitMasterAddress()
{
    if (!FLAGS_master_address.empty()) {
        return Status::OK();
    }
    // ETCD_MASTER_ADDRESS_TABLE was created in ETCD cluster manager.
    Status status = etcdStore_->CAS(ETCD_MASTER_ADDRESS_TABLE, MASTER_ADDRESS_KEY, "", workerAddr_);
    if (status.IsError()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            etcdStore_->Get(ETCD_MASTER_ADDRESS_TABLE, MASTER_ADDRESS_KEY, FLAGS_master_address),
            "Failed to get master address from etcd, and cas error:" + status.ToString());
    } else {
        FLAGS_master_address = workerAddr_;
    }
    LOG(INFO) << FormatString("The master address:%s of worker:%s", FLAGS_master_address, workerAddr_);
    return Status::OK();
}

void HashRing::RestoreScalingTaskIfNeeded(bool isRestartScenario)
{
    if (!IsWorkable()) {
        return;
    }

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (state_ == PRE_LEAVING) {
        SubmitMigrateDataTaskIfNeed(ringInfo_);
    }
    if (ringInfo_.add_node_info().empty() && ringInfo_.del_node_info().empty()) {
        return;
    }

    taskExecutor_->RestoreScalingTask(ringInfo_, isRestartScenario);
}

Status HashRing::InitWithoutEtcd(bool isMultiReplicaEnable, const std::string &hashRing)
{
    isMultiReplicaEnable_ = isMultiReplicaEnable;
    FLAGS_master_address = workerAddr_;
    startUpState_ = StartUpState::RESTART;
    HashRingPb hashRingPb;
    if (!hashRingPb.ParseFromString(hashRing)) {
        return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
    }
    auto workerInRing = hashRingPb.workers().find(workerAddr_);
    CHECK_FAIL_RETURN_STATUS(
        workerInRing != hashRingPb.workers().end(), K_RUNTIME_ERROR,
        "The etcd is not writable and the local worker is not in the persistent ring. Please check the etcd.");
    workerUuid_ = workerInRing->second.worker_uuid();
    taskExecutor_ = std::make_unique<HashRingTaskExecutor>(workerAddr_, workerUuid_, etcdStore_, isMultiReplicaEnable_);
    RETURN_IF_NOT_OK(UpdateWhenNodeRestart(hashRing, hashRingPb));
    RestoreScalingTaskIfNeeded(true);
    if (state_.load() != NO_INIT) {
        hashRingHealthCheck_ = std::make_unique<HashRingHealthCheck>(this);
        hashRingHealthCheck_->Init();
    }
    return Status::OK();
}

Status HashRing::InitWithEtcd(bool isMultiReplicaEnable)
{
    INJECT_POINT("HashRing.Init.ChangeDefaultHashTokenNum", [](int hashTokenNum) {
        HashRingAllocator::defaultHashTokenNum = hashTokenNum;
        return Status::OK();
    });
    // If initWorkerNum <= 0, do not return. Nodes rely on hashring to tell isRestart.
    LOG(INFO) << "HashRing start to init for worker:" << workerAddr_;
    isMultiReplicaEnable_ = isMultiReplicaEnable;
    if (FLAGS_etcd_address.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(InitMasterAddress());

    Timer timer;
    int waitTimeMs = 60'000;
    INJECT_POINT_NO_RETURN("HashRing.InitWithEtcd.MotifyWaitTimeMs",
                           [&waitTimeMs](int timeoutMs) { waitTimeMs = timeoutMs; });
    const int retryIntervalMs = 200;
    Status status;
    std::string oldVersionRingVal;
    TryGetOldRing(oldVersionRingVal);
    RangeSearchResult res;
    while (status = etcdStore_->CAS(ETCD_RING_PREFIX, "",
                                    std::bind(&HashRing::InitRing, this, std::placeholders::_1, std::placeholders::_2,
                                              std::placeholders::_3, oldVersionRingVal),
                                    res),
           status.GetCode() == K_TRY_AGAIN && timer.ElapsedMilliSecond() < waitTimeMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
    };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(status, "InitRing failed");
    baselineModRevisionOfRing_ = res.modRevision;
    taskExecutor_ = std::make_unique<HashRingTaskExecutor>(workerAddr_, workerUuid_, etcdStore_, isMultiReplicaEnable_);
    timer_ = std::make_unique<Timer>();

    (void)etcdStore_->Delete(ETCD_RING_PREFIX, FLAGS_etcd_address);
    // reset the state for centralized master scenario
    // In the centralized master scenario, we don't use the hash ring, but we still need to set uuid to etcd to
    // distinguish the startup state like what we do in HashRing::InitRing above. This state will be used in etcd
    // cluster manager later.
    if (IsCentralized()) {
        state_ = NO_INIT;
    }

    bool isRestart = false;
    RETURN_IF_NOT_OK(IsRestart(isRestart));
    LOG(INFO) << "HashRing finished init for worker:" << workerAddr_ << ", isRestart:" << isRestart
              << ", isRunning: " << IsRunning() << ", isWorkable:" << IsWorkable()
              << ", baseline: " << baselineModRevisionOfRing_.load() << ", workerId: " << workerUuid_;

    RestoreScalingTaskIfNeeded(true);
    if (state_.load() != NO_INIT) {
        hashRingHealthCheck_ = std::make_unique<HashRingHealthCheck>(this);
        hashRingHealthCheck_->Init();
    }
    return Status::OK();
}

Status HashRing::UpdateWhenNodeRestart(const std::string &oldValue, const HashRingPb &oldRing)
{
    // restore the initialized ring
    if (oldRing.cluster_has_init() || IsCentralized()) {
        // force update. make the hash ring work as early as possible.
        RETURN_IF_NOT_OK(UpdateRing(oldValue, -1, true));
        LOG(INFO) << "The hash ring started successfully after restoration.";
    }
    return Status::OK();
}

void HashRing::TryGetOldRing(std::string &oldVersionRingVal)
{
    while (!exitFlag_) {
        auto status = etcdStore_->Get(ETCD_RING_PREFIX, FLAGS_etcd_address, oldVersionRingVal);
        if (status.GetCode() == K_NOT_FOUND || status.IsOk()) {
            break;
        } else {
            static const int INTERVAL_MS_GET_KEY = 200;
            std::this_thread::sleep_for(std::chrono::milliseconds(INTERVAL_MS_GET_KEY));
        }
    }
}

Status HashRing::InitRing(const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry,
                          const std::string &oldVersionRingVal)
{
    (void)retry;
    HashRingPb oldRing;
    CHECK_FAIL_RETURN_STATUS(oldRing.ParseFromString(oldValue.empty() ? oldVersionRingVal : oldValue), K_RUNTIME_ERROR,
                             "ParseFromString failed");
    auto lines = SplitRingJson(FormatString("InitRing for worker %s, the old ring is ", workerAddr_), oldRing);
    std::for_each(lines.begin(), lines.end(), [](const std::string &line) { VLOG(1) << line; });

    // Restart is allowed only after node scaling down is completed.
    static const int INTERVAL_MS = 1000;
    if (oldRing.del_node_info().find(workerAddr_) != oldRing.del_node_info().end()) {
        LOG(INFO) << "The scale down of this node is being executed, retry...";
        auto intrevalMs = INTERVAL_MS;
        INJECT_POINT("HashRing.InitRing.CheckQuickly", [&](uint32_t timeMs) {
            intrevalMs = timeMs;
            return Status::OK();
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(intrevalMs));
        return Status(K_TRY_AGAIN, "The scale down of this node is being executed, retry...");
    }

    RETURN_IF_NOT_OK(UpdateWhenNodeRestart(oldValue, oldRing));

    HashRingPb newRing = oldRing;
    INJECT_POINT("worker.InitRing", [this, &newRing] {
        newRing.mutable_workers()->erase(workerAddr_);
        return Status::OK();
    });

    // set uuid and get the restart state
    newRing.set_cluster_id("");
    auto reusedUuid = GetReusedUuid(newRing);
    auto workerInRing = newRing.mutable_workers()->find(workerAddr_);
    if (workerInRing == newRing.mutable_workers()->end()) {
        startUpState_ = StartUpState::START;
        workerUuid_ = reusedUuid.empty() ? GetStringUuid() : reusedUuid;
        INJECT_POINT("HashRing.InitRing.CustomWorkerId", [this](const std::string &customizedWorkerId) {
            workerUuid_ = customizedWorkerId;
            return Status::OK();
        });
        WorkerPb workerPb;
        // If the reusedUuid is not empty, set the workerPb uuid after the uuid metadata is migrated back.
        if (reusedUuid.empty()) {
            workerPb.set_worker_uuid(workerUuid_);
        }
        workerPb.set_state(WorkerPb::INITIAL);
        (void)newRing.mutable_workers()->insert({ workerAddr_, workerPb });
    } else {
        startUpState_ = StartUpState::RESTART;
        workerUuid_ = workerInRing->second.worker_uuid();
        // After the uuid metadata is migrated in voluntary scale down, will clear the worker uuid, so reused uuid.
        workerUuid_ = workerUuid_.empty() ? reusedUuid : workerUuid_;
        // A node that should be scaled in restarts before the scale-in starts, the scale-in should be canceled.
        if (workerInRing->second.need_scale_down() && workerInRing->second.state() == WorkerPb::ACTIVE) {
            LOG(INFO) << "Cancel the unstarted scale-in task of " << workerAddr_;
            workerInRing->second.set_need_scale_down(false);
        }
    }
    if (workerUuid_.empty()) {
        std::for_each(lines.begin(), lines.end(), [](const std::string &line) { LOG(INFO) << line; });
        RETURN_STATUS(K_RUNTIME_ERROR, "worker id can not be empty!");
    }

    // not only the scale-up node but the first init nodes are regarded as new nodes, which will cause rocksdb erasure.
    if (newRing.workers().at(workerAddr_).state() == WorkerPb::INITIAL) {
        isNewNode_ = true;
    }

    if (!google::protobuf::util::MessageDifferencer::Equals(newRing, oldRing)) {
        newValue = std::make_unique<std::string>(newRing.SerializeAsString());
        auto lines = SplitRingJson(FormatString("InitRing for worker %s, the new ring is", workerAddr_), newRing);
        std::for_each(lines.begin(), lines.end(), [](const std::string &line) { VLOG(1) << line; });
    }
    return Status::OK();
}

bool HashRing::HashTokensIsReady(const HashRingPb &ring) const
{
    int workerNum = ring.workers_size();
    int hashTokenNum = workerNum * HashRingAllocator::defaultHashTokenNum;
    int sum = 0;
    for (const auto &worker : ring.workers()) {
        sum += worker.second.hash_tokens_size();
    }
    return sum == hashTokenNum;
}

void HashRing::GetActiveWorkersDbNames(std::vector<std::string> &activeDbNames)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &info : workerAddr2UuidMap_) {
        activeDbNames.emplace_back(info.second);
    }
}

void HashRing::TryFirstInit()
{
    if (HashTokensIsReady(ringInfo_)) {
        return;
    }

    // first init, generate all hash tokens.
    VLOG(LOG_LEVEL) << "Trying to generate hash tokens.";
    auto status = etcdStore_->CAS(
        ETCD_RING_PREFIX, "",
        [this](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            HashRingPb oldRing;
            if (!oldRing.ParseFromString(oldValue)) {
                LOG(WARNING) << "Failed to parse HashRingPb from string. give up and wait for next time if needed.";
                return Status::OK();
            }
            if (oldRing.cluster_has_init()) {
                VLOG(LOG_LEVEL) << "cluster has init.";
                return Status::OK();
            }
            std::set<std::string> sortedWorkers = GetKeysFromPairsContainer(oldRing.workers());
            if (sortedWorkers.empty()) {
                LOG(WARNING) << "Not found worker from HashRingPb. give up and wait for next time if needed.";
                return Status::OK();
            }
            // In order to reduce the CAS confliction, only the top MAX_CANDIDATE_WORKER_NUM workers are responsible for
            // GenerateAllHashTokens
            std::set<std::string> candidateWorkers(sortedWorkers.begin(),
                                                  
                std::next(sortedWorkers.begin(),
                          std::min(sortedWorkers.size(), static_cast<size_t>(MAX_CANDIDATE_WORKER_NUM))));
            if (!ContainsKey(candidateWorkers, workerAddr_)) {
                return Status::OK();
            }
            HashRingPb newRing = HashRingAllocator::GenerateAllHashTokens(oldRing, sortedWorkers);
            newValue = std::make_unique<std::string>(newRing.SerializeAsString());
            VLOG(LOG_LEVEL) << "TryFirstInit generates new ring.";
            return Status::OK();
        });
    if (status.IsError()) {
        LOG(ERROR) << "Update hash ring failed: " << status.GetMsg();
        return;
    }
    return;
}

void HashRing::TryAdd()
{
    INJECT_POINT("worker.HashRing.TryAdd", [] {});
    LOG_IF_ERROR(AddNode(ringInfo_), "Add node to working hash ring failed.");
}

void HashRing::EraseUnFinishMigrateDataWorker(HashRingPb &oldRing, std::unique_ptr<std::string> &newValue)
{
    HashRingPb newRing = oldRing;
    for (const auto &worker : oldRing.workers()) {
        // if workerid, hashtoken is empty and state is leaving, the node is voluntary scale down and meta migrate
        // finished, if all node exist, the node no need migrate data.
        if (worker.second.worker_uuid().empty() && worker.second.hash_tokens().empty()
            && worker.second.state() == WorkerPb::LEAVING) {
            newRing.mutable_workers()->erase(worker.first);
        }
    }
    newValue = std::make_unique<std::string>(newRing.SerializeAsString());
}

void HashRing::GenerateVoluntaryScaleDownChangingInfo()
{
    INJECT_POINT("InspectAndProcessPeriodically.skip", []() { return; });
    std::unordered_set<std::string> allScaleDownWorkers;
    uint32_t runningWorkerSize = 0;
    auto needRemoveWorkers = GetLeavingWorkers(ringInfo_, allScaleDownWorkers, runningWorkerSize);
    if (needRemoveWorkers.empty()) {
        return;
    }
    auto funcHandler = [&](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &) {
        HashRingPb oldRing;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(oldRing.ParseFromString(oldValue), K_RUNTIME_ERROR,
                                             "Failed to parse HashRingPb from string");
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        google::protobuf::util::MessageDifferencer differencer;
        differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
        if (!differencer.Compare(ringInfo_, oldRing)) {
            LOG(WARNING) << "local hashring not update, wait and try again";
            return Status::OK();
        }
        if (runningWorkerSize == allScaleDownWorkers.size()) {
            LOG(WARNING) << "Can't find standby worker, there is only one worker left in the cluster, shutdown now";
            taskExecutor_->ClearVoluntaryTaskId();
            // all node need exit, if node has unfinished migrate data job, no node can receive data, so mark migrate
            // data finish here.
            EraseUnFinishMigrateDataWorker(oldRing, newValue);
            allWorkersVoluntaryScaleDown_ = true;
            voluntaryScaleDownDone_ = true;
            return Status::OK();
        }
        HashRingAllocator allocator(oldRing);
        for (const auto &workerId : needRemoveWorkers) {
            auto &worker = oldRing.mutable_workers()->at(workerId);
            if (worker.state() == WorkerPb::ACTIVE) {
                LOG(INFO) << "Begin to generate voluntary scale add_node_info for " << workerId;
                worker.set_state(WorkerPb::LEAVING);
            } else if (worker.state() == WorkerPb::LEAVING) {
                INJECT_POINT("hashring.regenerate.sleep");
                LOG(INFO) << "Regenerate voluntary scale add_node_info for " << workerId;
            }
            std::string standbyWorker;
            if (!worker.worker_uuid().empty()) {
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                    GetStandbyWorkerExceptNoLock(worker.worker_uuid(), allScaleDownWorkers, standbyWorker),
                    "Cannot find standby node for " + workerId);
            }
            INJECT_POINT("GetStandbyWorkerExceptNoLock", [&standbyWorker](std::string addr) {
                standbyWorker = addr;
                return Status::OK();
            });
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                allocator.RemoveNodeVoluntarily(workerId, standbyWorker, hashFunction_(worker.worker_uuid()),
                                                allScaleDownWorkers, oldRing),
                "RemoveNodeVoluntarily failed");
            if (workerId == workerAddr_) {
                // If the voluntary scale down node is not the current node, there is no need to clean up
                // VoluntaryTaskId and re-execute the migration data task.
                taskExecutor_->ClearVoluntaryTaskId();
            }
        }
        newValue = std::make_unique<std::string>(oldRing.SerializeAsString());
        return Status::OK();
    };
    HASH_RING_LOG_IF_ERROR(etcdStore_->CAS(ETCD_RING_PREFIX, "", funcHandler), " generate voluntary info failed");
    return;
}

bool HashRing::IsPreLeaving(const std::string &workerAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto worker : ringInfo_.workers()) {
        if (worker.second.need_scale_down() == true && worker.first == workerAddr) {
            return true;
        }
    }
    return false;
}

bool HashRing::IsLeaving(const std::string &workerAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto worker : ringInfo_.workers()) {
        if (worker.second.state() == WorkerPb::LEAVING && worker.first == workerAddr) {
            return true;
        }
    }
    return false;
}

void HashRing::InspectAndProcessPeriodically()
{
    if (state_.load() == NO_INIT) {
        return;
    }

    INJECT_POINT("HashRing.HealthProbe", []() { ResetHealthProbe(); });

    std::shared_lock<std::shared_timed_mutex> lock(mutex_, std::defer_lock);
    bool rec = lock.try_lock();
    if (!rec) {
        return;
    }
    bool hasInit = ringInfo_.cluster_has_init();
    if (state_.load() == INIT && !hasInit) {
        int startWaitTime = 5;
        INJECT_POINT("test.start.notWait", [&startWaitTime](int waitTime) { startWaitTime = waitTime; });
        TryGenerateHashRange(startWaitTime, std::bind(&HashRing::TryFirstInit, this));
        return;
    }

    if (state_.load() == INIT && hasInit) {
        TryAdd();
    }

    if (state_.load() == PRE_RUNNING) {
        TryGenerateHashRange(FLAGS_add_node_wait_time_s, std::bind(&HashRing::TryAdd, this));
    }

    if ((state_.load() == RUNNING || state_.load() == PRE_LEAVING) && needVoluntaryScaleDown_) {
        HostPort migrateDbAddr;
        std::string migrateDbName;
        auto status =
            HashRingEvent::GetDbPrimaryLocation::GetInstance().NotifyAll(workerAddr_, migrateDbAddr, migrateDbName);
        if (status.IsOk() && migrateDbAddr.ToString() != workerAddr_) {
            LOG(INFO) << "wait voluntary scale down node change primary replica to local";
            return;
        }
        GenerateVoluntaryScaleDownChangingInfo();
        SubmitMigrateDataTaskIfNeed(ringInfo_);
    }

    if (state_.load() == RUNNING) {
        ClearWorkerMapOnInterval();
    }
}

void HashRing::TryGenerateHashRange(int waitTime, std::function<void()> func)
{
    if (!timer_ || timer_->ElapsedSecond() < waitTime) {
        return;
    }
    func();
}

std::set<std::string> HashRing::GetAddingWorkers(const HashRingPb &ring) const
{
    if ((!needForceJoin_ && ring.del_node_info_size() != 0) || ring.add_node_info_size() != 0) {
        // Scale up or scale down is happening, wait for next time.
        return {};
    }

    std::set<std::string> workers;
    for (auto &i : ring.workers()) {
        bool hasToken = i.second.hash_tokens_size() != 0;
        // 1. scale up is happening
        if (i.second.state() == WorkerPb::JOINING && !needForceJoin_) {
            return {};
        }
        // 2. scale down is happening, and it holds token and exists add_node_info (metadata migration is not complete).
        if (i.second.state() == WorkerPb::LEAVING && hasToken && !i.second.worker_uuid().empty() && !needForceJoin_) {
            return {};
        }

        if (!hasToken && i.second.state() == WorkerPb::INITIAL) {
            workers.emplace(i.first);
        }
    }

    return workers;
}

std::unordered_set<std::string> HashRing::GetLeavingWorkers(const HashRingPb &ring,
                                                            std::unordered_set<std::string> &allScaleDownWorkers,
                                                            uint32_t &runningWorkersSize) const
{
    if (voluntaryScaleDownDone_) {
        return {};
    }
    if (ring.del_node_info_size() != 0 || ring.add_node_info_size() != 0) {
        // Scale up or scale down is happening, wait for next time.
        return {};
    }

    std::unordered_set<std::string> workers;
    bool isScaleUp = false;
    std::unordered_set<std::string> leavingWorkers;
    for (auto &i : ring.workers()) {
        if (i.second.state() != WorkerPb::INITIAL) {
            runningWorkersSize++;
        }
        if (i.second.state() != WorkerPb::ACTIVE && i.second.state() != WorkerPb::LEAVING) {
            // scale up is happening, return leavingWorkers.
            isScaleUp = true;
            continue;
        }
        // for not finish migrate meta leaving workers, regenarate addNodeInfo.
        if (i.second.state() == WorkerPb::LEAVING
            && (!i.second.worker_uuid().empty() || !i.second.hash_tokens().empty())) {
            leavingWorkers.emplace(i.first);
        }
        if (i.second.need_scale_down()) {
            allScaleDownWorkers.emplace(i.first);
            if (!i.second.worker_uuid().empty() || !i.second.hash_tokens().empty()) {
                workers.emplace(i.first);
            }
        }
    }
    if (isScaleUp) {
        return leavingWorkers;
    }
    return workers;
}

Status HashRing::AddNode(const HashRingPb &currRing)
{
    auto workers = GetAddingWorkers(currRing);
    if (workers.empty()) {
        return Status::OK();
    }

    LOG(INFO) << "add nodes to working hash ring: " << VectorToString(workers);

    HashRingPb newRing = currRing;
    HashRingAllocator allocator(currRing);
    for (auto &i : workers) {
        std::vector<uint32_t> newNodeTokens;
        RETURN_IF_NOT_OK(allocator.AddNode(i, HashRingAllocator::defaultHashTokenNum, newNodeTokens));
        for (auto token : newNodeTokens) {
            (*newRing.mutable_workers())[i].mutable_hash_tokens()->Add(std::move(token));
        }
        (*newRing.mutable_workers())[i].set_state(WorkerPb::JOINING);
    }

    std::map<std::string, ChangeNodePb> addNodeInfos;
    allocator.GetAddNodeInfo(addNodeInfos);
    for (auto &info : addNodeInfos) {
        (*newRing.mutable_add_node_info())[info.first] = std::move(info.second);
    }
    AddUpgradeRange(newRing, workers);
    RangeSearchResult res;
    RETURN_IF_NOT_OK(etcdStore_->CAS(
        ETCD_RING_PREFIX, "",
        [&newRing, &currRing](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            HashRingPb oldRing;
            if (!oldRing.ParseFromString(oldValue)) {
                LOG(WARNING) << "Failed to parse HashRingPb from string. give up and wait for next time if needed.";
                return Status::OK();
            }
            google::protobuf::util::MessageDifferencer differencer;
            differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
            if (!differencer.Compare(oldRing, currRing)) {
                VLOG(1) << "ring has changed. give up and wait for next time if needed.";
                return Status::OK();
            }

            newValue = std::make_unique<std::string>(newRing.SerializeAsString());
            return Status::OK();
        },
        res));
    LOG_IF(INFO, res.modRevision > 0) << "[Ring] Write the scale up task of " << VectorToString(workers)
                                      << " into etcd success with version " << res.modRevision;
    return Status::OK();
}

Status HashRing::GetPrimaryWorkerUuid(const std::string &key, std::string &outWorkerUuid,
                                      std::optional<RouteInfo> &routeInfo) const
{
    uint32_t hash = hashFunction_(key);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    std::string workerAddr;
    RETURN_IF_NOT_OK(GetPrimaryWorkerAddrNoLock(hash, workerAddr, routeInfo));
    return GetUuidByWorkerAddrNoLock(workerAddr, outWorkerUuid);
}

Status HashRing::GetPrimaryWorkerAddr(uint32_t keyHash, std::string &outWorkerAddr) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    std::optional<RouteInfo> routeInfo;
    return GetPrimaryWorkerAddrNoLock(keyHash, outWorkerAddr, routeInfo);
}

Status HashRing::GetPrimaryWorkerAddrNoLock(uint32_t keyHash, std::string &outWorkerAddr,
                                            std::optional<RouteInfo> &routeInfo) const
{
    if (tokenMap_.empty()) {
        RETURN_STATUS(K_NOT_READY, "HashRing not ready, the token map is empty.");
    }
    auto iter = tokenMap_.upper_bound(keyHash);
    if (iter != tokenMap_.end()) {
        if (routeInfo) {
            routeInfo->currHashRingVersion = currHashRingVersion_;
            routeInfo->payload = std::make_pair(
                iter == tokenMap_.begin() ? tokenMap_.rbegin()->first : std::prev(iter)->first, iter->first);
        }
        VLOG(1) << "GetPrimaryWorker for hash:" << keyHash << ", the result token:" << iter->first
                << ", the result worker is:" << iter->second;
        outWorkerAddr = iter->second;
        return Status::OK();
    }
    VLOG(1) << "GetPrimaryWorker for hash:" << keyHash << ", the result token:" << tokenMap_.begin()->first
            << ", the result worker is:" << tokenMap_.begin()->second;
    if (routeInfo) {
        routeInfo->currHashRingVersion = currHashRingVersion_;
        routeInfo->payload = std::make_pair(tokenMap_.rbegin()->first, tokenMap_.begin()->first);
    }
    outWorkerAddr = tokenMap_.begin()->second;
    return Status::OK();
}

Status HashRing::HandleRingEvent(const mvccpb::Event &event, const std::string &prefix)
{
    if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        return Status::OK();
    }

    if (hashRingHealthCheck_ != nullptr) {
        hashRingHealthCheck_->UpdateRing(event.kv().value(), event.kv().mod_revision());
    }

    if (event.kv().key() == (prefix + "/")) {
        RETURN_IF_NOT_OK_APPEND_MSG(UpdateRing(event.kv().value(), event.kv().mod_revision()),
                                    "UpdateRing failed when WatchKey");
    }
    return Status::OK();
}

bool HashRing::CheckAllAddNodeFinishWhenSrcFailed(const std::string &workerAddr,
                                                  const std::unordered_set<std::string> &failedWorkers)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);

    // check only if the input workerAddr is the responsible node to migrate meta
    if (ringInfo_.add_node_info().empty()
        || ringInfo_.add_node_info().find(workerAddr) != ringInfo_.add_node_info().end()) {
        return true;
    }

    // check if scale-up migration is complete while src worker dead.
    for (auto &i : ringInfo_.add_node_info()) {
        for (auto &range : i.second.changed_ranges()) {
            if (!range.finished() && failedWorkers.find(range.workerid()) == failedWorkers.end()) {
                return false;
            }
        }
    }
    return true;
}

bool HashRing::IsInRange(const HashRange &ranges, const std::string &objKey, const std::string &dbName)
{
    std::string workerId;
    if (TrySplitWorkerIdFromObjecId(objKey, workerId).IsOk()) {
        uint32_t hashVal1 = hashFunction_(dbName);
        uint32_t hashVal2 = hashFunction_(workerId);
        uint32_t hashVal3 = 0;
        {
            std::shared_lock<std::shared_timed_mutex> lock(mutex_);
            auto substitute = ringInfo_.key_with_worker_id_meta_map().find(workerId);
            if (substitute != ringInfo_.key_with_worker_id_meta_map().end()) {
                auto standbyWorkerAddr = substitute->second;
                auto it = workerAddr2UuidMap_.find(standbyWorkerAddr);
                if (it != workerAddr2UuidMap_.end()) {
                    hashVal3 = hashFunction_(it->second);
                }
            }
        }

        for (auto &range : ranges) {
            // First equal to the second represent the migration of uuid object
            // - hashVal1: voluntary node Migrates all objects with uuid metadata, including those migrated from other
            // workers with different uuids.
            // - hashVal2: Migrates objects with uuid metadata back to the original worker (upgrade scenarios).
            // - hashVal3: for other node to migrate all objects with uuid metadata, including those migrated from other
            // workers with different uuids.
            if (range.first == range.second
                && (range.first == hashVal1 || range.first == hashVal2 || range.first == hashVal3)) {
                return true;
            }
        }
        return false;
    }

    return HashInRange(ranges, objKey);
}

bool HashRing::HashInRange(const HashRange &ranges, const std::string &objKey)
{
    uint32_t hash = hashFunction_(objKey);
    for (auto &range : ranges) {
        if (range.first > range.second) {
            if (hash > range.first || hash <= range.second) {
                return true;
            }
        } else {
            if (hash > range.first && hash <= range.second) {
                return true;
            }
        }
    }
    return false;
}

void HashRing::RecoverMigrationTask(const std::string &node)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (ringInfo_.add_node_info().find(node) != ringInfo_.add_node_info().end()) {
        LOG(INFO) << "Recover the scale up task for " << node;
        taskExecutor_->SubmitOneScaleUpTask({ node, ringInfo_.add_node_info().at(node) }, true);
    }
}

bool HashRing::CheckWorkerIsScaleDown(const std::string &workerAddr) const
{
    std::set<std::string> workers;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (ringInfo_.workers().find(workerAddr) == ringInfo_.workers().end()
        || ringInfo_.del_node_info().find(workerAddr) != ringInfo_.del_node_info().end()) {
        return true;
    }
    return false;
}

Status HashRing::ProcessUpdateRingEventIfRunning(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    // scale up begin.
    if (IncrementalAddNodeInfo(oldRing, newRing)) {
        return taskExecutor_->SubmitScaleUpTask(newRing);
    }

    /**
     * scale down begin.
     * scale down task must before scale up finish, recover may be failed if scale down begin
     * when scale up finish.
     */
    if (IncrementalDelNodeInfo(oldRing, newRing)) {
        return taskExecutor_->SubmitScaleDownTask(newRing);
    }

    // scale up end. update tokenMap_
    if (!oldRing.add_node_info().empty() && newRing.add_node_info().empty()) {
        LOG(INFO) << "Update tokens due to scale up finish: "
                  << VectorToString(GetKeysFromPairsContainer(oldRing.add_node_info()));
        taskExecutor_->SubmitVoluntaryRecoveryAsyncTask(oldRing, newRing);
    }

    return Status::OK();
}

Status HashRing::ProcessUpdateRingEventIfPreparing(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    /**
     * scale down begin.
     * scale down task must before scale up finish, recover may be failed if scale down begin
     * when scale up finish.
     */
    if (IncrementalDelNodeInfo(oldRing, newRing)) {
        return taskExecutor_->SubmitScaleDownTask(newRing);
    }

    // scale-up node, PRE_RUNNING -> RUNNING
    if (!oldRing.add_node_info().empty() && newRing.add_node_info().empty()) {
        taskExecutor_->SubmitVoluntaryRecoveryAsyncTask(oldRing, newRing);
    }

    return Status::OK();
}

bool HashRing::CheckReceiveMigrateInfo(const std::string &workerAddr)
{
    INJECT_POINT("hashRing.noNeedToCheckForTest", []() { return true; });
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto addNodeInfoExist = ringInfo_.add_node_info().find(workerAddr) != ringInfo_.add_node_info().end();
    if (!isMultiReplicaEnable_) {
        return addNodeInfoExist;
    }
    bool delNodeInfoExist = false;
    for (const auto &infos : ringInfo_.del_node_info()) {
        for (const auto &info : infos.second.changed_ranges()) {
            if (info.workerid() == workerAddr) {
                delNodeInfoExist = true;
            }
        }
    }
    return addNodeInfoExist || delNodeInfoExist;
}

static std::string GetUnProcessedNodeInfo(const google::protobuf::Map<std::string, datasystem::ChangeNodePb> &nodeInfo)
{
    std::string fullLog;
    for (auto &pair : nodeInfo) {
        std::string detail;
        for (auto &range : pair.second.changed_ranges()) {
            if (!range.finished()) {
                detail += range.workerid() + ",";
            }
        }
        fullLog += FormatString("{%s:[%s]}, ", pair.first, detail);
    }
    return fullLog;
}

std::string HashRing::SummarizeHashRing(const HashRingPb &ring)
{
    int activeNum = 0;
    int joiningNum = 0;
    int initNum = 0;
    int leavingNum = 0;
    std::string joiningNodes;
    std::string leavingNodes;
    for (auto &i : ring.workers()) {
        if (i.second.state() == WorkerPb::ACTIVE) {
            ++activeNum;
        } else if (i.second.state() == WorkerPb::JOINING) {
            ++joiningNum;
            joiningNodes += i.first + ',';
        } else if (i.second.state() == WorkerPb::INITIAL) {
            ++initNum;
        } else if (i.second.state() == WorkerPb::LEAVING) {
            ++leavingNum;
            leavingNodes += i.first + ',';
        }
    }
    return FormatString(
        "Ring summarize: total:%d, active:%d, joining:%d(%s), initial:%d, leaving:%d(%s) add_node_info: %d(%s), "
        "del_node_info: %d(%s), key_with_worker_id_meta_map: %d",
        ring.workers_size(), activeNum, joiningNum, joiningNodes, initNum, leavingNum, leavingNodes,
        ring.add_node_info_size(), GetUnProcessedNodeInfo(ring.add_node_info()), ring.del_node_info_size(),
        GetUnProcessedNodeInfo(ring.del_node_info()), ring.key_with_worker_id_meta_map_size());
}

void HashRing::SubmitMigrateDataTaskIfNeed(const HashRingPb &ring)
{
    if (taskExecutor_->IsMigrateDataTaskRunning()) {
        return;
    }
    auto iter = ring.workers().find(workerAddr_);
    if (iter == ring.workers().end()) {
        return;
    }
    if (iter->second.worker_uuid().empty() && iter->second.hash_tokens().empty()
        && iter->second.state() == WorkerPb::LEAVING) {
        const int logInterval = 10;
        LOG_EVERY_T(INFO, logInterval) << "Worker " << workerAddr_
                                       << " meta data migrate finish, begin to trigger data migration.";
        if (HashRingEvent::DataMigrationReady::GetInstance().NotifyAll().IsOk()) {
            dataMigrationStarted_ = true;
            LOG_IF_ERROR(taskExecutor_->SubmitMigrateDataTask(), "Migrate data task failed");
        }
    }
}

void HashRing::ProcessUpdateRingEventIfLeaving(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    if (newRing.workers().find(workerAddr_) == newRing.workers().end()) {
        int waitTimeMs = 1000;  // Wait 1000ms let other worker receive hash ring event before worker shutdown
        std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
        voluntaryScaleDownDone_ = true;
    }

    /**
     * scale down begin.
     * scale down task must before scale up finish, recover may be failed if scale down begin
     * when scale up finish.
     */
    if (IncrementalDelNodeInfo(oldRing, newRing)) {
        HASH_RING_LOG_IF_ERROR(taskExecutor_->SubmitScaleDownTask(newRing), "scale down task failed");
    }

    if (IncrementalAddNodeInfo(oldRing, newRing)) {
        LOG(INFO) << "Begin to migrate voluntary scale down meta data.";
        INJECT_POINT("hashRing.voluntary.sleep", [](int time) {
            std::this_thread::sleep_for(std::chrono::seconds(time));
            return;
        });
        HASH_RING_LOG_IF_ERROR(taskExecutor_->SubmitScaleUpTask(newRing), "scale up task failed");
    }
    SubmitMigrateDataTaskIfNeed(newRing);
}

bool HashRing::SkipUpdateRing(const HashRingPb &newRing, int64_t version, bool forceUpdate)
{
    if (!forceUpdate && version <= baselineModRevisionOfRing_.load()) {
        LOG(INFO) << "Ignore ring update of version " << version
                  << " because it's out-of-date. Ring summarize is: " << SummarizeHashRing(newRing);
        return true;
    }

    // the baselineModRevisionOfRing_ only fliter the version before baseline, it's not necessary to restrict it after
    // the new version arrives. Also to prevent rollover issues.
    if (!forceUpdate) {
        baselineModRevisionOfRing_ = INT64_MIN;
    }

    if (state_ == FAIL) {
        LOG(INFO) << "The node has failed. Wait for exiting, will not update ring anymore. Version: " << version;
        return true;
    }

    return false;
}

void HashRing::UpdateLocalRing(const HashRingPb &newRing, bool forceUpdate)
{
    // 1. update ring
    ringInfo_.CopyFrom(newRing);
    UpdateTokenMap();
    // 2. update state_ according to ringInfo_
    UpdateLocalState(forceUpdate);
}

Status HashRing::UpdateRing(const std::string &newSerializedRingInfo, int64_t version, bool forceUpdate)
{
    INJECT_POINT("HashRing.UpdateRing.sleep");
    HashRingPb newRing;
    if (!newRing.ParseFromString(newSerializedRingInfo)) {
        return Status(K_RUNTIME_ERROR, "Failed to parse RingInfoPb");
    }
    HashRingPb oldRing;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        RETURN_OK_IF_TRUE(SkipUpdateRing(newRing, version, forceUpdate));
        currHashRingVersion_++;
        LOG(INFO) << "Update ring of version " << version << ". " << SummarizeHashRing(newRing);
        auto lines = SplitRingJson(FormatString("Worker %s update local hash ring to", workerAddr_), newRing);
        std::for_each(lines.begin(), lines.end(), [](const std::string &line) { LOG(INFO) << line; });

        INJECT_POINT("HashRing.NotReceiveUpdate", [&oldRing, &newRing](uint32_t time_s) {
            if (oldRing.add_node_info().empty() && !newRing.add_node_info().empty()) {
                std::this_thread::sleep_for(std::chrono::seconds(time_s));
            }
            return Status::OK();
        });

        // update member according to the new ring
        oldRing = std::move(ringInfo_);
        UpdateLocalRing(newRing, forceUpdate);
    }

    if (!forceUpdate && !oldRing.cluster_has_init() && newRing.cluster_has_init()) {
        ProcessForClusterInit();
    }

    ProcessForScaleDownFinish(oldRing, newRing);

    ProcessForScaleupFinish(oldRing, newRing);

    // 1. no need to process update ring task if this node is ready to exit.
    // 2. if the node is just started, task will be restored in one time
    // 3. NO_INIT
    if (forceUpdate || state_ == FAIL || state_ == NO_INIT) {
        return Status::OK();
    }

    // 3. notify other components to remove the no longer existed workers
    if (EnableAutoDelDeadNode()) {
        HashRingEvent::SyncClusterNodes::GetInstance().NotifyAll(GetKeysFromPairsContainer(newRing.workers()));
    }

    Status status = Status::OK();
    switch (state_) {
        case NO_INIT:
            break;
        case PRE_LEAVING:
            ProcessUpdateRingEventIfLeaving(oldRing, newRing);
            break;
        case RUNNING:
            status = ProcessUpdateRingEventIfRunning(oldRing, newRing);
            break;
        case INIT:  // fall through
        case PRE_RUNNING:
            status = ProcessUpdateRingEventIfPreparing(oldRing, newRing);
            break;
        case FAIL:
            break;
    }

    return status;
}

void HashRing::ProcessForClusterInit()
{
    std::string nextWorkerUuid;
    Status rc = GetNextWorker(workerUuid_, nextWorkerUuid);
    if (rc.IsError()) {
        LOG(ERROR) << "Get next worker failed:" << rc.ToString();
    }
    LOG(INFO) << "Cluster init finish, current worker uuid:" << workerUuid_ << ", next worker uuid:" << nextWorkerUuid;
    // Notification is required even if the next node cannot be found.
    HashRingEvent::ClusterInitFinish::GetInstance().NotifyAll(workerUuid_, nextWorkerUuid);
}

Status HashRing::ProcessForScaleDownFinish(HashRingPb &oldRing, HashRingPb &newRing)
{
    std::vector<std::string> needDeleteDbNames;
    auto oldWorkerInfos = GetWorkersFromHashRingPb(oldRing);
    for (const auto &worker : oldWorkerInfos) {
        if (newRing.workers().contains(worker.first)) {
            continue;
        }
        std::string dbName = worker.second;
        if (dbName.empty()) {
            LOG(WARNING) << "The uuid is empty for " << worker.first << " and not exists in update_worker_map";
            continue;
        }
        needDeleteDbNames.emplace_back(dbName);
    }
    if (!needDeleteDbNames.empty()) {
        HashRingEvent::ScaleDownFinish::GetInstance().NotifyAll(needDeleteDbNames);
    }
    return Status::OK();
}

void HashRing::ProcessForScaleupFinish(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    for (const auto &worker : oldRing.workers()) {
        if (worker.second.state() != WorkerPb::JOINING) {
            continue;
        }
        auto iter = newRing.workers().find(worker.first);
        if (iter == newRing.workers().end()) {
            LOG(WARNING) << "worker " << worker.first << " not exists in new ring.";
            continue;
        }
        if (iter->second.state() == WorkerPb::ACTIVE) {
            LOG(INFO) << "Worker " << worker.first << " uuid:" << iter->second.worker_uuid() << " scaleup finish.";
            HashRingEvent::ScaleupFinish::GetInstance().NotifyAll(iter->second.worker_uuid());
        }
    }
}

bool HashRing::ChangeStateTo(HashState newState, std::string log)
{
    static auto toString = [](HashState state) {
        switch (state) {
            case NO_INIT:
                return "NO_INIT";
            case INIT:
                return "INIT";
            case PRE_RUNNING:
                return "PRE_RUNNING";
            case RUNNING:
                return "RUNNING";
            case PRE_LEAVING:
                return "PRE_LEAVING";
            case FAIL:
                return "FAIL";
            default:
                return "UNKNOWN";
        }
    };
    if (newState != state_.load()) {
        if (state_ == FAIL) {
            LOG(ERROR) << FormatString(
                "Skip convert hash ring local state from %s to %s because FAIL state is the terminate state.",
                toString(state_.load()), toString(newState));
            return false;
        }
        LOG(INFO) << FormatString("Convert hash ring local state from %s to %s. %s", toString(state_.load()),
                                  toString(newState), log);
        state_ = newState;
        return true;
    }
    return false;
}

// the mapping from HashRingPb::WorkerPb::StatePb to HashRing::HashState
void HashRing::UpdateLocalState(bool forceUpdate)
{
    if (IsCentralized()) {
        ChangeStateTo(NO_INIT);
        return;
    }
    if (state_ == FAIL) {
        LOG(INFO) << "Skip update local state because this node is ready to quit.";
        return;
    }

    auto iter = ringInfo_.workers().find(workerAddr_);
    if (iter == ringInfo_.workers().end()) {
        if (state_ == PRE_LEAVING) {
            LOG(INFO) << "voluntary remove from hash ring success.";
            return;
        }
        if (forceUpdate && ringInfo_.cluster_has_init()) {
            ChangeStateTo(PRE_RUNNING);
            return;
        }
        ChangeStateTo(FAIL, "The current node cannot find in the hash ring anymore.");
        return;
    }
    if (ContainsKey(ringInfo_.del_node_info(), workerAddr_)) {
        ChangeStateTo(FAIL, "There is a running scale down task on this node.");
        return;
    }

    auto stateInEtcd = iter->second.state();
    if (stateInEtcd == WorkerPb::ACTIVE) {
        ChangeStateTo(RUNNING);
    } else if (stateInEtcd == WorkerPb::LEAVING) {
        ChangeStateTo(PRE_LEAVING);
    } else if ((stateInEtcd == WorkerPb::INITIAL || stateInEtcd == WorkerPb::JOINING) && ringInfo_.cluster_has_init()) {
        ChangeStateTo(PRE_RUNNING);
    } else {
        ChangeStateTo(INIT);
    }
    if (iter->second.need_scale_down()) {
        if (forceUpdate && stateInEtcd == WorkerPb::ACTIVE) {
            LOG(WARNING) << "Scale-in task that have not started will no longer be executed after restart";
        } else {
            needVoluntaryScaleDown_ = true;
        }
    }
}

void HashRing::UpdateTokenMap()
{
    decltype(tokenMap_) tmpTokenMap;
    decltype(workerUuidHashMap_) tmpWorkerUuidHashMap;
    decltype(workerUuid2AddrMap_) tmpWorkerId2AddrMap;
    decltype(relatedWorkerMap_) tmpRelatedWorkerMap;
    decltype(workerAddr2UuidMap_) tmpAddrToWorkerIdMap;

    // 1. regenerate
    for (const auto &kv : ringInfo_.workers()) {
        if (kv.second.state() != WorkerPb::ACTIVE && kv.second.state() != WorkerPb::LEAVING) {
            continue;
        }
        const auto &workerId = kv.first;
        for (auto token : kv.second.hash_tokens()) {
            tmpTokenMap.insert({ token, workerId });
        }
        if (kv.second.worker_uuid().empty()) {
            continue;
        }
        uint32_t hash = hashFunction_(kv.second.worker_uuid());
        tmpWorkerUuidHashMap.insert({ hash, workerId });
    }
    GenerateHashRingUuidMap(ringInfo_, tmpWorkerId2AddrMap, tmpAddrToWorkerIdMap, tmpRelatedWorkerMap);

    // 2. assign if modify
    std::stringstream log;
    if (tokenMap_ != tmpTokenMap) {
        tokenMap_ = std::move(tmpTokenMap);
        log << "Update token map to size: " << tokenMap_.size() << ". ";
        SaveHashRange();
    }
    if (workerUuidHashMap_ != tmpWorkerUuidHashMap) {
        workerUuidHashMap_ = std::move(tmpWorkerUuidHashMap);
        log << "Update workerid hash map to size: " << workerUuidHashMap_.size() << ". ";
    }
    if (tmpAddrToWorkerIdMap != workerAddr2UuidMap_ || tmpWorkerId2AddrMap != workerUuid2AddrMap_
        || tmpRelatedWorkerMap != relatedWorkerMap_) {
        workerAddr2UuidMap_ = std::move(tmpAddrToWorkerIdMap);
        workerUuid2AddrMap_ = std::move(tmpWorkerId2AddrMap);
        relatedWorkerMap_ = std::move(tmpRelatedWorkerMap);
        log << "Update {workerAddr, workerId} map to size: " << workerAddr2UuidMap_.size() << ". "
            << "Update {workerId, workerAddr} map for route to size:" << workerUuid2AddrMap_.size() << ". ";
    }
    // 3. print log
    if (log.rdbuf()->in_avail() > 0) {
        LOG(INFO) << log.str();
    }
}

bool HashRing::NeedRedirect(const std::string &objKey, HostPort &masterAddr)
{
    if (IsCentralized()) {
        return false;
    }
    // If the key carries workerId, we need to check whether redirection is required only when the migration of meta
    // with workerId occurs. There are currently some scenarios where the above situation will occur:
    // 1. The local node is scaling down voluntarily.
    // 2. The workerId is in update_worker_map, which means that this workerId may be reused after a node is restarted.
    // If this happens, the local node will migrate the metadata with workerId to the restarted node.
    // 3. Following the above situation 2, after completing the migration task, the source node will remove the target
    // node's workerId from update_worker_map and update the hash ring. If the source node of the request has not yet
    // updated to this version of the hash ring and sent the request, local node still needs to reply that redirection
    // is required. Considering this situation, the key carrying the workerId cannot short-circuit the following
    // redirection judgment logic.

    // check if in add_node_info, return new node if true
    {
        HashRange ranges;
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        for (auto &i : ringInfo_.add_node_info()) {
            for (auto &r : i.second.changed_ranges()) {
                ranges.emplace_back(r.from(), r.end());
            }
            if (IsInRange(ranges, objKey, workerUuid_)) {
                LOG_IF_ERROR(masterAddr.ParseString(i.first), "parse failed: " + i.first);
                VLOG(1) << "adding node, return new master: " << masterAddr.ToString();
                return true;
            }
            ranges.clear();
        }
    }

    // check if hash master has changed or not due to a possible scale-up finished.
    HostPort currMetaAddr;
    auto status = GetMasterAddr(objKey, currMetaAddr);
    if (status.IsError() || currMetaAddr.ToString() == workerAddr_) {
        LOG_IF_ERROR(status, "The server fails to check the master address and does not redirect.");
        return false;
    }
    VLOG(1) << "add node finish, return new master";
    masterAddr = currMetaAddr;
    return true;
}

Status HashRing::GetWorkerAddrByUuidForAddressing(const std::string &workerUuid, HostPort &workerAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return ::worker::GetWorkerAddrByUuidForAddressing(ringInfo_, workerUuid2AddrMap_, workerUuid, workerAddr);
}

Status HashRing::GetWorkerAddrByUuidForMultiReplica(const std::string &workerUuid, HostPort &workerAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return ::worker::GetWorkerAddrByUuidForMultiReplica(ringInfo_, workerUuid2AddrMap_, workerUuid, workerAddr);
}

Status HashRing::GetUuidByWorkerAddr(const std::string &workerAddr, std::string &uuid)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return GetUuidByWorkerAddrNoLock(workerAddr, uuid);
}

Status HashRing::GetUuidByWorkerAddrNoLock(const std::string &workerAddr, std::string &uuid) const
{
    auto it = workerAddr2UuidMap_.find(workerAddr);
    if (it != workerAddr2UuidMap_.end()) {
        uuid = it->second;
        return Status::OK();
    }
    auto updateIt = ringInfo_.update_worker_map().find(workerAddr);
    CHECK_FAIL_RETURN_STATUS(updateIt != ringInfo_.update_worker_map().end(), K_NOT_FOUND,
                             "Cannot find node with addr " + workerAddr);
    uuid = updateIt->second.worker_uuid();
    return Status::OK();
}

std::string HashRing::GetLocalWorkerUuid() const
{
    return workerUuid_;
}

Status HashRing::GetMasterAddr(const std::string &objKey, HostPort &masterAddr)
{
    std::string masterUuid;
    RETURN_IF_NOT_OK(GetMasterUuid(objKey, masterUuid));
    return GetWorkerAddrByUuidForAddressing(masterUuid, masterAddr);
}

Status HashRing::GetMasterUuid(const std::string &objKey, std::string &masterUuid)
{
    // To make sure hash ring is running
    RETURN_IF_NOT_OK(WaitWorkable());
    if (TrySplitWorkerIdFromObjecId(objKey, masterUuid).IsError()) {
        std::optional<RouteInfo> routeInfo;
        RETURN_IF_NOT_OK(GetPrimaryWorkerUuid(objKey, masterUuid, routeInfo));
    }
    return Status::OK();
}

void HashRing::SaveHashRange()
{
    std::unique_lock<std::mutex> l(hashRangeMutex_);
    HashPosition curr = 0;
    bool needExtra = false;
    for (auto iter = tokenMap_.begin(); iter != tokenMap_.end(); ++iter) {
        if (iter->second == workerAddr_) {
            needExtra |= (curr == 0);
            auto beg = curr;
            auto end = iter->first == 0 ? 0 : iter->first - 1;
            if (beg == 0 && end == 0) {
                continue;
            }
            hashRange_.emplace_back(beg, end);
        }
        curr = iter->first;
    }
    if (needExtra) {
        hashRange_.emplace_back(curr, UINT32_MAX);
    }
}

bool HashRing::NeedToTryRemoveWorker(const std::string &workerAddr, const std::unordered_set<std::string> &failWorkers)
{
    if (workerAddr == workerAddr_) {
        LOG_FIRST_N(ERROR, 1)
            << "Maybe suffered a passive scale down due to network connectivity issues or current worker not "
               "exists in hash ring. Restart.";
        SetUnhealthy();
        if (state_.load() == PRE_LEAVING || voluntaryScaleDownDone_) {
            INJECT_POINT("notExit", [] { return false; });
            voluntaryScaleDownDone_ = true;
            (void)raise(SIGTERM);
        } else {
            // In passive scaling down scenarios, send SIGKILL to stop the service to prevent asynchronous tasks from
            // getting stuck and unable to stop.
            // For passive scaling down caused by a fault, we believe that the asynchronous task has processed the time
            // configured by NODE_DEAD_TIMEOUT, but the task still fails, and there is no point in continuing to wait.
            std::shared_lock<std::shared_timed_mutex> lock(mutex_);
            auto workerInRing = ringInfo_.workers().find(workerAddr_);
            auto workerInRingStr =
                workerInRing == ringInfo_.workers().end() ? "NOT_FOUND" : workerInRing->second.ShortDebugString();
            LOG(ERROR) << FormatString(
                "Worker is ready for passive scaling down, worker state_ is: %d, worker in ring: %s, del_node_info: "
                "%s",
                state_.load(), workerInRingStr, MapToString(ringInfo_.del_node_info()));
            lock.unlock();

            Provider::Instance().FlushLogs();

            (void)raise(SIGKILL);
        }
        return false;
    }

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    // removal from hash ring has finished.
    if (!ContainsKey(ringInfo_.workers(), workerAddr)) {
        LOG(INFO) << workerAddr << " no longer exist in hash ring, try to remove from the cluster node table";
        lock.unlock();
        return false;
    }
    // removal from hash ring is processing.
    if (ContainsKey(ringInfo_.del_node_info(), workerAddr)) {
        return false;
    }

    // When src failed, check if all worker finish migrate, and remove src worker.
    return CheckAllAddNodeFinishWhenSrcFailed(workerAddr, failWorkers);
}

void HashRing::RemoveWorkers(const std::unordered_set<std::string> &workers)
{
    if (!EnableAutoDelDeadNode()) {
        return;
    }
    bool expected = true;
    (void)needForceJoin_.compare_exchange_strong(expected, false);

    Status rc;
    // check if should remove itself
    if (state_ == FAIL || (voluntaryScaleDownDone_ && !allWorkersVoluntaryScaleDown_)) {
        rc = RemoveWorker(workerAddr_, workers);
        if (rc.GetCode() == StatusCode::K_INVALID) {
            LOG(WARNING) << "Remove worker " << workerAddr_ << " failed, detail: " << rc.ToString();
            return;
        }
    }
    // check if should remove failed worker
    for (auto &i : workers) {
        rc = RemoveWorker(i, workers);
        WARN_IF_ERROR(rc, FormatString("Remove failed worker %s failed. All failed workers: %s",
                                                             i, VectorToString(workers)));
        if (rc.GetCode() == StatusCode::K_INVALID) {
            LOG(WARNING) << "Remove worker " << i << " failed, detail: " << rc.ToString();
            return;
        }
    }
}

static void LogOnce(const std::string &flag, const std::string &log)
{
    static std::string lastFlag;
    static std::vector<std::string> lastLogs;
    if (lastFlag == flag) {
        if (ContainsKey(lastLogs, log)) {
            return;
        }
    } else {
        lastFlag = flag;
        lastLogs.clear();
    }
    lastLogs.push_back(log);
    LOG(INFO) << log;
}

Status HashRing::GetWorkerUuidFromRing(const std::string &workerAddr, std::string &uuid)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = ringInfo_.workers().find(workerAddr);
    if (iter == ringInfo_.workers().end()) {
        return Status(K_NOT_FOUND, FormatString("Can't found the worker on ring", workerAddr));
    }
    uuid = iter->second.worker_uuid();
    return Status::OK();
}

Status HashRing::RemoveWorker(const std::string &workerAddr, const std::unordered_set<std::string> &failWorkers)
{
    RETURN_OK_IF_TRUE(!NeedToTryRemoveWorker(workerAddr, failWorkers));

    std::string uuidOfRemoveWorker;
    if (GetWorkerUuidFromRing(workerAddr, uuidOfRemoveWorker).IsError()) {
        // has been removed
        return Status::OK();
    }
    std::set<std::string> processWorkers;
    RETURN_IF_NOT_OK(GetProcessWorkerForRemoveWorker(uuidOfRemoveWorker, failWorkers, processWorkers));
    INJECT_POINT("ProcessWorkerIsLocal", [&processWorkers, this]() {
        processWorkers.emplace(workerAddr_);
        return Status::OK();
    });
    if (!ContainsKey(processWorkers, workerAddr_)) {
        LogOnce(VectorToString(failWorkers),
                FormatString("select [%s] to write the del_node_info of [%s]. skip..(failed workers: %s)",
                             VectorToString(std::vector<std::string>(processWorkers.begin(), processWorkers.end())),
                             workerAddr, VectorToString(failWorkers)));
        return Status::OK();
    }

    VLOG(1) << "Try to write the scale down task of " << workerAddr << " into etcd";
    RangeSearchResult res;
    auto ret = etcdStore_->CAS(
        ETCD_RING_PREFIX, "",
        [this, &workerAddr, &uuidOfRemoveWorker](const std::string &oldValue, std::unique_ptr<std::string> &newValue,
                                                 bool &retry) {
            HashRingPb currRing;
            if (!currRing.ParseFromString(oldValue)) {
                return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
            }
            // If the ring in etcd is newer than ringInfo_, return and try next time. Maybe no need to remove.
            std::shared_lock<std::shared_timed_mutex> lock(mutex_);
            google::protobuf::util::MessageDifferencer differencer;
            differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
            if (!differencer.Compare(ringInfo_, currRing)) {
                retry = false;
                return Status(K_INVALID, "ringInfo is not latest");
            }
            auto worker = currRing.mutable_workers()->find(workerAddr);
            if (worker == currRing.mutable_workers()->end()
                || currRing.del_node_info().find(workerAddr) != currRing.del_node_info().end()) {
                return Status::OK();
            }

            if (worker->second.state() == WorkerPb::INITIAL) {
                currRing.mutable_del_node_info()->insert({ workerAddr, {} });
            } else {
                auto rc = HashRingAllocator(currRing).RemoveNode(workerAddr, currRing);
                if (rc.IsError()) {
                    LOG(WARNING) << FormatString("Remove worker %s failed: %s.", workerAddr, rc.ToString());
                    // Wait the init nodes join the ring so that the failed node can be removed.
                    needForceJoin_ = true;
                    if (needVoluntaryScaleDown_) {
                        // Only scale down worker can handle fault worker, which means there are no available nodes, so
                        // exit as soon as possible.
                        LOG(WARNING) << "Give up voluntary scale down due to no available nodes.";
                        voluntaryScaleDownDone_ = true;
                    }
                    return Status::OK();
                }
            }

            if (!uuidOfRemoveWorker.empty()) {
                (*currRing.mutable_key_with_worker_id_meta_map())[uuidOfRemoveWorker] = workerAddr_;
            } else {
                LOG(INFO) << FormatString("workerId of %s is empty, skip update key_with_worker_id_meta_map",
                                          workerAddr);
            }

            if (google::protobuf::util::MessageDifferencer::Equals(ringInfo_, currRing)) {
                LOG(INFO) << "skip write the repeat ring into etcd";
                return Status::OK();
            }
            newValue = std::make_unique<std::string>(currRing.SerializeAsString());
            INJECT_POINT("hashRing.removeWorker");
            return Status::OK();
        },
        res);
    LOG_IF(INFO, res.modRevision > 0) << "[Ring] Write the scale down task of " << workerAddr
                                      << " into etcd success with version " << res.modRevision << ".";
    return ret;
}

Status HashRing::GetHashRingWorkerNum(int &workerNum, bool isFromOtherAz) const
{
    workerNum = 0;
    if (!IsCentralized()) {
        CHECK_FAIL_RETURN_STATUS(IsWorkable(), K_NOT_READY, "HashRing is not workable. Call again later.");
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto findLocalWorker = ringInfo_.workers().find(workerAddr_) != ringInfo_.workers().end();

        if (!isFromOtherAz) {
            CHECK_FAIL_RETURN_STATUS(
                findLocalWorker, K_NOT_READY,
                FormatString("Local worker not in hashring, worker not ready. workerAddr: %s", workerAddr_));
        }

        for (auto &i : ringInfo_.workers()) {
            if (ringInfo_.del_node_info().find(i.first) == ringInfo_.del_node_info().end()) {
                workerNum++;
            }
        }
    } else {
        static const int DEFAULT_NUM_CENTRALIZED = -1;
        workerNum = DEFAULT_NUM_CENTRALIZED;
    }
    return Status::OK();
}

std::set<std::string> HashRing::GetValidWorkersInHashRing() const
{
    std::set<std::string> workers;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &i : ringInfo_.workers()) {
        if (ringInfo_.del_node_info().find(i.first) == ringInfo_.del_node_info().end()) {
            workers.emplace(i.first);
        }
    }
    return workers;
}

std::set<std::string> HashRing::GetWorkersInDelNodeInfo() const
{
    std::set<std::string> workers;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &i : ringInfo_.del_node_info()) {
        workers.emplace(i.first);
    }
    return workers;
}

Status HashRing::GetStandbyWorkerByAddr(const std::string &workerAddr, std::string &nextWorker)
{
    std::string uuid;
    RETURN_IF_NOT_OK(GetUuidByWorkerAddr(workerAddr, uuid));
    return GetStandbyWorkerByUuid(uuid, nextWorker);
}

Status HashRing::GetStandbyWorkerByUuid(const std::string &uuid, std::string &standbyWorker)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    uint32_t hash = hashFunction_(uuid);
    auto iter = workerUuidHashMap_.upper_bound(hash);
    if (iter == workerUuidHashMap_.end()) {
        auto smallest = workerUuidHashMap_.upper_bound(0);
        if (smallest == workerUuidHashMap_.end() || smallest->first == hash) {
            return Status(K_RUNTIME_ERROR,
                          FormatString("Standby worker not found on the ring, localWorker is (%s).", workerAddr_));
        }
        standbyWorker = smallest->second;
    } else {
        standbyWorker = iter->second;
    }
    return Status::OK();
}

Status HashRing::GetStandbyWorker(std::string &standbyWorker)
{
    return GetStandbyWorkerByUuid(workerUuid_, standbyWorker);
}

Status HashRing::GetActiveWorkers(uint32_t num, std::vector<std::string> &activeWorkers)
{
    activeWorkers.clear();
    if (num == 0) {
        RETURN_STATUS(StatusCode::K_INVALID, "required number is 0");
    }

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (ringInfo_.workers().empty()) {
        constexpr int logInterval = 30;
        LOG_EVERY_T(WARNING, logInterval) << "worker in hash ring is empty";
        return Status::OK();
    }
    auto currIt = ringInfo_.workers().begin();
    uint32_t randomStep = RandomData().GetRandomUint32(0, ringInfo_.workers_size() - 1);
    std::advance(currIt, randomStep);
    for (auto it = currIt; it != ringInfo_.workers().end(); ++it) {
        if (it->first == workerAddr_) {
            continue;
        }
        if (it->second.state() != WorkerPb::ACTIVE || it->second.need_scale_down()
            || ringInfo_.del_node_info().find(it->first) != ringInfo_.del_node_info().end()) {
            continue;
        }
        activeWorkers.emplace_back(it->first);
        if (activeWorkers.size() >= num) {
            break;
        }
    }
    if (activeWorkers.size() >= num) {
        return Status::OK();
    }
    for (auto it = ringInfo_.workers().begin(); it != currIt; ++it) {
        if (it->first == workerAddr_) {
            continue;
        }
        if (it->second.state() != WorkerPb::ACTIVE || it->second.need_scale_down()
            || ringInfo_.del_node_info().find(it->first) != ringInfo_.del_node_info().end()) {
            continue;
        }
        activeWorkers.emplace_back(it->first);
        if (activeWorkers.size() >= num) {
            break;
        }
    }
    return Status::OK();
}

Status HashRing::GetStandbyWorkerExceptNoLock(const std::string &uuid,
                                              const std::unordered_set<std::string> &excludeAddrs,
                                              std::string &standbyWorker)
{
    Status err =
        Status(K_RUNTIME_ERROR, FormatString("Standby worker not found on the ring, workerUuid is (%s).", uuid));
    uint32_t hash = hashFunction_(uuid);
    auto iter = workerUuidHashMap_.upper_bound(hash);
    if (iter == workerUuidHashMap_.end()) {
        iter = workerUuidHashMap_.begin();
    }
    auto startIter = iter;
    do {
        if (excludeAddrs.find(iter->second) == excludeAddrs.end() && iter->first != hash) {
            standbyWorker = iter->second;
            return Status::OK();
        }
        iter++;
        if (iter == workerUuidHashMap_.end()) {
            iter = workerUuidHashMap_.begin();
        }
    } while (iter != startIter);

    return err;
}

Status HashRing::GetProcessWorkerForRemoveWorker(const std::string &uuid,
                                                 const std::unordered_set<std::string> &failedWorkers,
                                                 std::set<std::string> &processWorkerAddrs)
{
    std::string processWorkerUuid = uuid;
    std::string processWorkerAddr;
    Status rc;
    const std::unordered_set<WorkerPb::StatePb> states{ WorkerPb::ACTIVE, WorkerPb::LEAVING, WorkerPb::JOINING,
                                                        WorkerPb::INITIAL };
    for (auto i = 0; i < MAX_CANDIDATE_WORKER_NUM; i++) {
        rc = GetRelatedWorkerImpl(processWorkerUuid, states, failedWorkers, true, processWorkerUuid, processWorkerAddr);
        if (rc.IsError()) {
            break;
        }
        processWorkerAddrs.emplace(processWorkerAddr);
    }
    if (processWorkerAddrs.empty()) {
        // The uuids of w1, w2, and w3 are all empty. w4 has a uuid but is faulty. Need to find a node to handle the
        // failure of w4 (handling method: when RemoveNode is invoked, because cannot find an available node,
        // mark local worker voluntary scale down status as done and then exit.)
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        for (const auto &worker : ringInfo_.workers()) {
            if (!ContainsKey(states, worker.second.state()) || ContainsKey(failedWorkers, worker.first)) {
                continue;
            }
            processWorkerAddrs.emplace(worker.first);
            break;
        }
    }
    return processWorkerAddrs.empty() ? rc : Status::OK();
}

Status HashRing::VoluntaryScaleDown()
{
    INJECT_POINT("worker.VoluntaryScaleDown");
    LOG(INFO) << "Begin to process voluntary scale down task";
    auto funcHandler = [&](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
        HashRingPb oldRing;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(oldRing.ParseFromString(oldValue), K_RUNTIME_ERROR,
                                             "Failed to parse HashRingPb from string");

        auto itr = oldRing.mutable_workers()->find(workerAddr_);
        if (itr == oldRing.workers().end()) {
            LOG(INFO) << "Can't find the worker in the hash ring, give up VoluntaryScaleDown.";
            return Status::OK();
        }

        itr->second.set_need_scale_down(true);
        newValue = std::make_unique<std::string>(oldRing.SerializeAsString());
        return Status::OK();
    };

    return etcdStore_->CAS(ETCD_RING_PREFIX, "", funcHandler);
}

bool HashRing::EnableAutoDelDeadNode() const
{
    return enableDistributedMaster_ && FLAGS_auto_del_dead_node;
}

Status HashRing::GetRelatedWorkerImpl(const std::string &currWorkerUuid,
                                      const std::unordered_set<WorkerPb::StatePb> &states,
                                      const std::unordered_set<std::string> &failedWorkers, bool getNextNode,
                                      std::string &outWorkerUuid, std::string &outWorkerAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(relatedWorkerMap_.size() >= 1, K_NOT_FOUND, "Insufficient number of workers.");
    // If the uuid is empty, it means that the metadata has been migrated and the data has not yet been migrated. Select
    // the first available node.
    auto iter = currWorkerUuid.empty() ? relatedWorkerMap_.begin() : relatedWorkerMap_.find(currWorkerUuid);
    // current worker must exists.
    if (iter == relatedWorkerMap_.end()) {
        return Status(K_NOT_FOUND, FormatString("The worker %s not found on the ring.", currWorkerUuid));
    }

    for (size_t i = 0; i <= relatedWorkerMap_.size(); i++) {
        iter = getNextNode ? LoopNext(relatedWorkerMap_, iter) : LoopPrev(relatedWorkerMap_, iter);
        if (iter == relatedWorkerMap_.end()) {
            continue;
        }
        // 1. check exists in hash ring.
        std::string workerAddr = iter->second.ToString();
        auto worker = ringInfo_.workers().find(workerAddr);
        if (worker == ringInfo_.workers().end()) {
            LOG(WARNING) << "Worker " << workerAddr << " not exists in ringInfo_";
            continue;
        }
        // 2. check state in hash ring.
        if (states.count(worker->second.state()) == 0) {
            continue;
        }
        // 3. standby worker can not be failed.
        if (failedWorkers.find(workerAddr) != failedWorkers.end()) {
            continue;
        }
        outWorkerUuid = iter->first;
        outWorkerAddr = std::move(workerAddr);
        return Status::OK();
    }
    return Status(K_NOT_FOUND, FormatString("The %s node of worker %s not found on the ring.",
                                            (getNextNode ? "next" : "prev"), currWorkerUuid));
}

Status HashRing::GetNextWorker(const std::string &currWorkerUuid, std::string &nextWorkerUuid)
{
    std::string nextWorkerAddr;
    return GetRelatedWorkerImpl(currWorkerUuid, { WorkerPb::ACTIVE, WorkerPb::LEAVING }, {}, true, nextWorkerUuid,
                                nextWorkerAddr);
}

Status HashRing::GetPrevWorker(const std::string &currWorkerUuid, std::string &prevWorkerUuid)
{
    std::string prevWorkerAddr;
    return GetRelatedWorkerImpl(currWorkerUuid, { WorkerPb::ACTIVE, WorkerPb::LEAVING }, {}, false, prevWorkerUuid,
                                prevWorkerAddr);
}

Status HashRing::GetUuidInCurrCluster(const std::string &oldUuid, std::string &newUuid,
                                      std::optional<RouteInfo> &routeInfo)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsUuid(oldUuid), K_INVALID,
                             FormatString("%s must be in the format of uuid", oldUuid));
    if (routeInfo) {
        routeInfo->payload = oldUuid;
    }
    newUuid.clear();
    RETURN_IF_NOT_OK(WaitWorkable());
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    auto iter1 = workerUuid2AddrMap_.find(oldUuid);
    if (iter1 != workerUuid2AddrMap_.end()) {
        newUuid = oldUuid;
        return Status::OK();
    }
    auto iter2 = ringInfo_.key_with_worker_id_meta_map().find(oldUuid);
    if (iter2 != ringInfo_.key_with_worker_id_meta_map().end()) {
        VLOG(1) << FormatString("node[%s] has been remove, try to rehash to other worker", oldUuid);
        auto iter3 = workerAddr2UuidMap_.find(iter2->second);
        if (iter3 != workerAddr2UuidMap_.end()) {
            newUuid = iter3->second;
            return Status::OK();
        }
        LOG(ERROR) << FormatString("The msg in ringInfo_[%s] and workerAddr2UuidMap_[%s] is inconsistent",
                                   MapToString(ringInfo_.key_with_worker_id_meta_map()),
                                   MapToString(workerAddr2UuidMap_));
        RETURN_STATUS(K_RUNTIME_ERROR, "Cannot find the master of " + oldUuid);
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("%s is not in this az", oldUuid));
}

std::string HashRing::GetReusedUuid(HashRingPb &ring) const
{
    const auto &iter = ring.update_worker_map().find(workerAddr_);
    if (iter != ring.update_worker_map().end()) {
        LOG(INFO) << FormatString("Worker %s reused uuid %s", workerAddr_, iter->second.worker_uuid());
        return iter->second.worker_uuid();
    }
    return "";
}

void HashRing::ClearWorkerMapOnInterval()
{
    static int count = 0;
    static const int intervalTimes = 10;
    count++;
    if (count % intervalTimes != 0) {
        return;
    }
    HashRingPb newRing = ringInfo_;
    if (!ClearWorkerMapIfNeed(newRing)) {
        return;
    }
    LOG_IF_ERROR(
        etcdStore_->CAS(
            ETCD_RING_PREFIX, "",
            [this, &newRing](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                HashRingPb oldRing;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(oldRing.ParseFromString(oldValue), K_RUNTIME_ERROR,
                                                     "Failed to parse HashRingPb from string");
                std::shared_lock<std::shared_timed_mutex> lock(mutex_);
                if (!google::protobuf::util::MessageDifferencer::Equals(ringInfo_, oldRing)) {
                    VLOG(1) << "ring has changed. give up and wait for next time if needed.";
                    return Status::OK();
                }
                newValue = std::make_unique<std::string>(newRing.SerializeAsString());
                return Status::OK();
            }),
        "Clear update_worker_map failed");
}

void HashRing::AddUpgradeRange(HashRingPb &ring, std::set<std::string> &workers)
{
    for (const auto &worker : workers) {
        auto uuidIt = ring.update_worker_map().find(worker);
        if (uuidIt == ring.update_worker_map().end()) {
            continue;
        }
        auto &uuid = uuidIt->second.worker_uuid();
        auto iter = ring.mutable_workers()->find(worker);
        if (iter != ring.mutable_workers()->end() && iter->second.worker_uuid().empty()) {
            iter->second.set_worker_uuid(uuid);
        }
        auto addrIt = ring.key_with_worker_id_meta_map().find(uuid);
        if (addrIt == ring.key_with_worker_id_meta_map().end()) {
            continue;
        }
        uint32_t hash = hashFunction_(uuid);
        ChangeNodePb::RangePb range;
        range.set_workerid(addrIt->second);
        range.set_from(hash);
        range.set_end(hash);
        range.set_finished(false);
        range.set_is_upgrade(true);
        (*ring.mutable_add_node_info())[worker].mutable_changed_ranges()->Add(std::move(range));
        VLOG(1) << FormatString("Begin to restore worker(%s) uuid meta(%s)", worker, uuid);
    }
}

Status HashRing::RemoveExpiredMap(const std::set<std::string> &expiredUuids)
{
    return etcdStore_->CAS(
        ETCD_RING_PREFIX, "",
        [&expiredUuids](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            HashRingPb currRing;
            if (!currRing.ParseFromString(oldValue)) {
                return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
            }
            for (auto &expiredUuid : expiredUuids) {
                currRing.mutable_key_with_worker_id_meta_map()->erase(expiredUuid);
            }
            newValue = std::make_unique<std::string>(currRing.SerializeAsString());
            return Status::OK();
        });
}

bool HashRing::IsInUpdateWorkerMap(const std::string &workerId)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    for (const auto &itr : ringInfo_.update_worker_map()) {
        if (itr.second.worker_uuid() == workerId) {
            return true;
        }
    }
    return false;
}

Status HashRing::WaitWorkable()
{
    RETURN_OK_IF_TRUE(IsWorkable());
    const int retryNumMax = 3;
    int retryNum = 0;
    static const int SLEEP_TIME_MS = 200;
    while (retryNum < retryNumMax) {
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        retryNum += 1;
        if (IsWorkable()) {
            break;
        } else if (!IsWorkable() && retryNum == retryNumMax) {
            RETURN_STATUS(K_NOT_READY, FormatString("Hash ring not ready[state: %d], get workerId failed", state_));
        }
    }
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem

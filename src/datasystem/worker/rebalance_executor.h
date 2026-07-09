/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Worker-side executor for memory rebalance tasks assigned by master.
 */
#ifndef DATASYSTEM_WORKER_REBALANCE_EXECUTOR_H
#define DATASYSTEM_WORKER_REBALANCE_EXECUTOR_H

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"
#include "datasystem/worker/object_cache/data_migrator/handler/migrate_data_handler.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/rebalance_candidate_provider.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {

struct RebalanceExecutorConfig {
    HostPort localAddress;
    // Non-owning. The owner must keep the cluster manager alive until RebalanceExecutor is destroyed.
    ClusterManager *clusterManager;
    std::shared_ptr<AkSkManager> akSkManager;
    std::shared_ptr<object_cache::ObjectTable> objectTable;
    std::shared_ptr<object_cache::WorkerOcEvictionManager> evictionManager;
    std::shared_ptr<WorkerMasterApiManagerBase<WorkerMasterOCApi>> apiManager;
};

class RebalanceExecutor {
public:
    using MigrateResult = object_cache::MigrateDataHandler::MigrateResult;

    explicit RebalanceExecutor(RebalanceExecutorConfig config);

    ~RebalanceExecutor() = default;

    /**
     * @brief Submit a task returned by master from ResourceReportRspPb.
     * @param[in] task The rebalance task assigned to this source worker.
     */
    void Submit(const master::RebalanceTaskPb &task);

#ifdef WITH_TESTS
    using SelectCandidatesHook = std::function<Status(uint64_t, std::unordered_map<std::string, uint64_t> &)>;
    using MigrateToTargetHook = std::function<MigrateResult(const master::RebalanceTaskPb &, const HostPort &,
                                                            const std::vector<std::string> &)>;
    using ReportResultHook = std::function<void(const master::RebalanceTaskPb &, master::RebalanceTaskStatusPb,
                                                uint64_t, uint64_t, uint64_t, const std::string &)>;

    void SetTestHooks(SelectCandidatesHook selectHook, MigrateToTargetHook migrateHook, ReportResultHook reportHook)
    {
        selectHook_ = std::move(selectHook);
        migrateHook_ = std::move(migrateHook);
        reportHook_ = std::move(reportHook);
    }

    bool IsRunningForTest() const
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        return running_;
    }

    std::string GetRunningTaskIdForTest() const
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        return runningTaskId_;
    }

    void SetRunningForTest(bool running, const std::string &runningTaskId)
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_ = running;
        runningTaskId_ = runningTaskId;
    }
#endif

private:
    struct ExecutionStats {
        master::RebalanceTaskStatusPb status = master::REBALANCE_TASK_FAILED;
        uint64_t migratedBytes = 0;
        uint64_t migratedObjects = 0;
        uint64_t failedObjects = 0;
        bool candidatesExhausted = false;
        std::string failedReason;
    };

    void Execute(master::RebalanceTaskPb task);
    void SubmitBusyResult(const master::RebalanceTaskPb &task, const std::string &runningTaskId);
    uint64_t BuildLocalDeadlineMs(const master::RebalanceTaskPb &task) const;
    uint64_t NowMsForExpiryCheck() const;
    bool IsExpired(uint64_t localDeadlineMs) const;
    Status ExecuteBatch(const master::RebalanceTaskPb &task, const HostPort &targetAddr, ExecutionStats &stats,
                        object_cache::DataMigrator &migrator);
    void ExecuteBatches(const master::RebalanceTaskPb &task, const HostPort &targetAddr, ExecutionStats &stats,
                        uint64_t localDeadlineMs);
    Status ValidateTask(const master::RebalanceTaskPb &task, HostPort &targetAddr, uint64_t localDeadlineMs) const;
    Status SelectCandidates(uint64_t maxBytes, std::unordered_map<std::string, uint64_t> &candidates);
    MigrateResult MigrateToTarget(const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                  const std::vector<std::string> &objectKeys,
                                  object_cache::DataMigrator &migrator);
    void ReportResult(const master::RebalanceTaskPb &task, master::RebalanceTaskStatusPb status, uint64_t migratedBytes,
                      uint64_t migratedObjects, uint64_t failedObjects, const std::string &failedReason);
    Status GetWorkerMasterApi(std::shared_ptr<WorkerMasterOCApi> &workerMasterApi) const;
    uint64_t CalculateMigratedBytes(const std::unordered_map<std::string, uint64_t> &candidates,
                                    const MigrateResult &result) const;
    void MarkTaskDone();

    HostPort localAddress_;
    ClusterManager *clusterManager_{ nullptr };
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<object_cache::ObjectTable> objectTable_{ nullptr };
    std::shared_ptr<object_cache::WorkerOcEvictionManager> evictionManager_{ nullptr };
    std::shared_ptr<WorkerMasterApiManagerBase<WorkerMasterOCApi>> apiManager_{ nullptr };
    object_cache::RebalanceCandidateProvider candidateProvider_;
    mutable std::mutex taskMutex_;
    bool running_{ false };
    std::string runningTaskId_;
#ifdef WITH_TESTS
    SelectCandidatesHook selectHook_;
    MigrateToTargetHook migrateHook_;
    ReportResultHook reportHook_;
#endif
    // Keep the worker pool as the last member so its destructor joins submitted tasks before any state captured by
    // those tasks is destroyed.
    ThreadPool executorPool_;
};

}  // namespace worker
}  // namespace datasystem
#endif

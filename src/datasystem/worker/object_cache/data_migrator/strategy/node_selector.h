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
 * Description: The node info for available memory resource.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_NODE_SELECTOR_H
#define DATASYSTEM_MIGRATE_DATA_NODE_SELECTOR_H

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class NodeSelector {
public:
    using RebalanceTaskHandler = std::function<void(const master::RebalanceTaskPb &)>;

    /**
     * @brief Get the singleton instance.
     * @return The singleton instance.
     */
    static NodeSelector &Instance();

    /**
     * @brief Init NodeSelector.
     * @param[in] localAddress The worker local address.
     * @param[in] clusterManager The pointer to cluster manager.
     * @param[in] apiManager The manager of worker master api.
     */
    void Init(const std::string &localAddress, ClusterManager *clusterManager,
              std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager);

    /**
     * Shutdown the NodeSelector and cleanup resources.
     */
    void Shutdown();

    /**
     * @brief Select node from other workers.
     * @param[in] excludeNodes The nodes don't be selected.
     * @param[in] preferNode The prior node that should be selected.
     * @param[in] needSize The size that the selected node should be larger than.
     * @param[out] outNode The selected node.
     */
    Status SelectNode(const std::unordered_set<std::string> &excludeNodes, const std::string &preferNode,
                      size_t needSize, std::string &outNode);

    /**
     * @brief Get the available memory size from the target worker.
     * @param[in] address The address that the target worker is.
     * @return The available memory size from the address worker.
     */
    size_t GetAvailableMemory(const std::string &address);

    /**
     * @brief Try to get the available memory size from the target worker.
     * @param[in] address The address that the target worker is.
     * @param[out] availableMemory The available memory size from the address worker.
     * @return K_OK if the resource snapshot has a ready target worker, otherwise the error status.
     */
    Status TryGetAvailableMemory(const std::string &address, size_t &availableMemory);

    /**
     * @brief Has enough available memory from all workers.
     * @brief[in] needMemory the need  memory.
     * @return If the sum available memory from all workers is larger than the needMemory, return true, else false.
     */
    bool HasEnoughAvailableMemory(size_t needMemory);

    /**
     * @brief Register the callback that consumes memory rebalance tasks returned by master.
     * @param[in] handler The callback implemented by worker-side rebalance executor.
     */
    void RegisterRebalanceTaskHandler(RebalanceTaskHandler handler);

    /**
     * @brief Clear the registered memory rebalance task callback.
     */
    void UnregisterRebalanceTaskHandler();
protected:
    NodeSelector();
    ~NodeSelector();
    /**
     * @brief Collect cluster info, report self memory info and get all workers info.
     * @return Status of this call.
     */
    Status CollectClusterInfo();

    /**
     * @brief Report resource.
     * @param[in] workerMasterApi The worker master api.
     * @param[in] req The request req.
     * @param[out] rsp The request response.
     * @return Status of this call.
     */
    Status ReportResource(const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                          master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp);

    /**
     * @brief Get worker master api.
     * @param[out] workerMasterApi The worker master api.
     * @return Status of this call.
     */
    Status GetWorkerMasterApi(std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi);

    /**
     * @brief Get standby worker.
     * @param[in] excludeNodes The nodes don't be selected.
     * @param[out] outNode The selected node.
     * @return Status of this call.
     */
    Status GetStandbyWorker(const std::unordered_set<std::string> &excludeNodes, std::string &outNode);

    /**
     * @brief Try to get available memory from the current resource snapshot.
     * @param[in] address The address that the target worker is.
     * @param[out] availableMemory The available memory size from the address worker.
     * @param[out] hasSnapshot Whether the resource snapshot already has any node information.
     * @return K_OK if the snapshot has a ready target worker, otherwise the error status.
     */
    Status TryGetAvailableMemoryFromSnapshot(const std::string &address, size_t &availableMemory,
                                             bool &hasSnapshot) const;

    mutable std::shared_timed_mutex  nodeInfosMutex_;
    std::vector<NodeInfo> rankList_;
    size_t totalSize_ = 0;
private:
    NodeSelector(const NodeSelector &) = delete;
    NodeSelector(NodeSelector &&) = delete;
    NodeSelector &operator=(const NodeSelector &) = delete;
    NodeSelector &operator=(NodeSelector &&) = delete;

    /**
     * @brief The worker thread.
     */
    void WorkerThread();

    std::string localAddress_;
    ClusterManager *clusterManager_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager_{ nullptr };

    std::atomic<bool> running_;
    Thread workerThread_;
    std::mutex taskMutex_;
    std::condition_variable taskCv_;
    std::mutex rebalanceTaskHandlerMutex_;
    RebalanceTaskHandler rebalanceTaskHandler_;
    std::atomic<bool> subSuccess_{ false };
    WaitPost subReadyPost_;
    struct Token {
        std::mutex mutex_;
        bool working = false;

        std::atomic<bool> alive{true};
    };
    Token* token_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif

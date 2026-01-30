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
#include <map>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class NodeSelector {
public:
    /**
     * @brief Get the singleton instance.
     * @return The singleton instance.
     */
    static NodeSelector &Instance();

    /**
     * @brief Init NodeSelector.
     * @param[in] localAddress The worker local address.
     * @param[in] etcdCM The pointer to etcd cluster manager.
     * @param[in] apiManager The manager of worker master api.
     */
    void Init(const std::string &localAddress, EtcdClusterManager *etcdCM,
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
     * @brief Has enough available memory from all workers.
     * @brief[in] needMemory the need  memory.
     * @return If the sum available memory from all workers is larger than the needMemory, return true, else false.
     */
    bool HasEnoughAvailableMemory(size_t needMemory);
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
    EtcdClusterManager *etcdCM_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager_{ nullptr };

    std::atomic<bool> running_;
    Thread workerThread_;
    std::mutex taskMutex_;
    std::condition_variable taskCv_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif
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

#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"

#include <string>
#include <vector>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
static const std::string RESOURCE_MONITOR_MASTER = "RESOURCE_MONITOR";
static const int64_t REPORT_RESOURCE_INTERVAL_TIME_MS = 30 * 1000;
static const int64_t REPORT_RESOURCE_INTERVAL_TIME_MS_IF_FAILED = 500;
NodeSelector &NodeSelector::Instance()
{
    static NodeSelector instance;
    return instance;
}

NodeSelector::NodeSelector() : running_(false)
{
}

NodeSelector::~NodeSelector()
{
    Shutdown();
}

void NodeSelector::Init(const std::string &localAddress, EtcdClusterManager *etcdCM,
                        std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager)
{
    if (!etcdCM || !apiManager) {
        LOG(WARNING) << "The etcdCM_ or apiManager_ is empty, can not set running and start worker thread";
        return;
    }
    if (running_.exchange(true)) {
        LOG(WARNING) << "NodeSelector already initialized";
        return;
    }
    localAddress_ = localAddress;
    etcdCM_ = etcdCM;
    apiManager_ = std::move(apiManager);
    running_.store(true);

    workerThread_ = Thread(&NodeSelector::WorkerThread, this);
    workerThread_.set_name("NodeSelector");
    LOG(INFO) << "NodeSelector initialized";
}

void NodeSelector::Shutdown()
{
    if (!running_.exchange(false)) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_.store(false);
    }
    taskCv_.notify_all();
    subReadyPost_.Set();

    if (workerThread_.joinable()) {
        workerThread_.join();
    }
    etcdCM_  = nullptr;
    apiManager_.reset();
    LOG(INFO) << "NodeSelector shutdown";
}

Status NodeSelector::SelectNode(const std::unordered_set<std::string> &excludeNodes, const std::string &preferNode,
                                size_t needSize, std::string &outNode)
{
    // 1. If rankList_ is empty, obtain Standby worker from EtcdClusterManager and return;
    // 2. If the maximum remaining capacity in rankList_ is less than 1MB, return K_NO_SPACE;
    // 3. If the remaining capacity of the preferNode > needSize, select it;
    // 4. Randomly select the top n (5) nodes with available capacity > needSize, excluding nodes in excludedNodes;
    // 5. The isReady flag indicates whether the node is in active scaling-down state;
    //   do not select nodes that are not ready.
    std::shared_lock<std::shared_timed_mutex> lock(nodeInfosMutex_);
    if (rankList_.empty()) {
        return GetStandbyWorker(excludeNodes, outNode);
    }
    auto maxLeftMemory = rankList_[0].availableMemory;
    CHECK_FAIL_RETURN_STATUS(maxLeftMemory > 1 * MB_TO_BYTES,
                             K_NO_SPACE, "The max available memory in not enough");

    auto it = std::find_if(rankList_.begin(), rankList_.end(),
                           [&preferNode](NodeInfo info) { return info.nodeId == preferNode; });
    if (it != rankList_.end() && it->isReady && it->availableMemory > needSize) {
        outNode = preferNode;
        return Status::OK();
    }

    uint64_t maxN = 5;
    std::vector<NodeInfo> maxNNodes;
    maxNNodes.reserve(maxN);
    std::string backupNode;
    for (const auto &nodeInfo : rankList_) {
        auto it = excludeNodes.find(nodeInfo.nodeId);
        if (it != excludeNodes.end()) {
            continue;
        }
        if (!nodeInfo.isReady) {
            break;
        }
        if (nodeInfo.availableMemory <= needSize) {
            backupNode = nodeInfo.nodeId;
            break;
        }
        maxNNodes.emplace_back(nodeInfo);
        if (maxNNodes.size() == maxN) {
            break;
        }
    }
    if (maxNNodes.empty() && !backupNode.empty()) {
        outNode = backupNode;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!maxNNodes.empty(), K_NOT_FOUND, "not find the profit node");
    // Randomly select one from maxNNodes as the result
    static thread_local std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::shuffle(maxNNodes.begin(), maxNNodes.end(), gen);
    outNode = maxNNodes.front().nodeId;
    return Status::OK();
}

Status NodeSelector::GetStandbyWorker(const std::unordered_set<std::string> &excludeNodes, std::string &outNode)
{
    std::string worker = localAddress_;
    int maxCount = 5;
    for (int i = 0; i < maxCount; ++i) {
        RETURN_IF_NOT_OK(etcdCM_->GetStandbyWorkerByAddr(worker, outNode));
        if (outNode == localAddress_) {
            outNode.clear();
            RETURN_STATUS(K_NOT_FOUND, "Not found the stand by worker");
        }
        auto it = excludeNodes.find(outNode);
        if (it == excludeNodes.end()) {
            return Status::OK();
        }
        worker = outNode;
        outNode.clear();
    }
    RETURN_STATUS(K_NOT_FOUND, "No key was found within the maxDepth loop count");
}

size_t NodeSelector::GetAvailableMemory(const std::string &address)
{
    const int waitReadyTimeoutMs = 1000;
    if (!subSuccess_.load() && running_.load()) {
        subReadyPost_.WaitFor(waitReadyTimeoutMs);
        if (!subSuccess_.load()) {
            return 0;
        }
    }
    std::shared_lock<std::shared_timed_mutex> lock(nodeInfosMutex_);
    auto it = std::find_if(rankList_.begin(), rankList_.end(),
                           [&address](NodeInfo info) {return info.nodeId == address; });
    if (it == rankList_.end()) {
        return 0;
    }
    if (!it->isReady) {
        return 0;
    }

    return it->availableMemory;
}

bool NodeSelector::HasEnoughAvailableMemory(size_t needMemory)
{
    std::shared_lock<std::shared_timed_mutex> lock(nodeInfosMutex_);
    return totalSize_ > needMemory;
}

void NodeSelector::WorkerThread()
{
    LOG(INFO) << "Start worker thread to periodically collect cluster info";
    int64_t intervalMs = REPORT_RESOURCE_INTERVAL_TIME_MS;
    INJECT_POINT_NO_RETURN("NodeSelector.setInterval", [&intervalMs](int interval) { intervalMs = interval; });
    while (running_) {
        auto rc = CollectClusterInfo();
        if (rc.IsError()) {
            LOG(WARNING) << "Collect cluster info failed, errMsg is " << rc.GetMsg();
        } else {
            subSuccess_.store(true);
            subReadyPost_.Set();
        }
        std::unique_lock<std::mutex> lock(taskMutex_);
        if (!running_.load()) {
            break;
        }
        (void)taskCv_.wait_for(
            lock, std::chrono::milliseconds(subSuccess_ ? intervalMs : REPORT_RESOURCE_INTERVAL_TIME_MS_IF_FAILED),
            [this]() { return !running_.load(); });
    }
}

Status NodeSelector::GetWorkerMasterApi(std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi)
{
    // get the master address info
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(etcdCM_->GetMetaAddress(RESOURCE_MONITOR_MASTER, metaAddrInfo));
    workerMasterApi = apiManager_->GetWorkerMasterApi(metaAddrInfo.GetAddress());
    if (workerMasterApi == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "The worker master api is nullptr");
    }
    return Status::OK();
}

Status NodeSelector::ReportResource(const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                    master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp)
{
    // Report current worker resource info to master
    master::WorkerStat *stat = req.mutable_stat();
    stat->set_address(localAddress_);
    stat->set_available_memory(datasystem::memory::Allocator::Instance()->GetMemoryAvailToHighWater());
    stat->set_is_ready(!(etcdCM_->CheckLocalNodeIsExiting()));
    return workerMasterApi->ReportResource(req, rsp);
}

Status NodeSelector::CollectClusterInfo()
{
    std::shared_ptr<worker::WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(GetWorkerMasterApi(workerMasterApi));
    // Report current worker resource info to master
    master::ResourceReportReqPb req;
    master::ResourceReportRspPb rsp;
    RETURN_IF_NOT_OK(ReportResource(workerMasterApi, req, rsp));
    // update the rankList_
    std::unique_lock<std::shared_timed_mutex> lock(nodeInfosMutex_);
    rankList_.clear();
    rankList_.reserve(rsp.stats().size());
    totalSize_ = 0;
    for (const auto &info : rsp.stats()) {
        rankList_.emplace_back(info.address(), info.available_memory(), info.is_ready());
        if (info.is_ready()) {
            totalSize_ += info.available_memory();
        }
    }
    std::sort(rankList_.begin(), rankList_.end(), [](const NodeInfo &a, const NodeInfo &b) { return b < a; });
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
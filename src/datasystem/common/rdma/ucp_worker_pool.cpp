/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: UcpWorkerPool class that wraps around and manages multiple
 * UcpWorkers. This class automatically handles reuses of UcpWorkers and provides
 * methods for removing info associated with a bad IP address
 */

#include "datasystem/common/rdma/ucp_worker_pool.h"

#include <mutex>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
UcpWorkerPool::UcpWorkerPool(const ucp_context_h &ucpContext, uint32_t workerN)
    : context_(ucpContext), workerN_(workerN)
{
}

UcpWorkerPool::~UcpWorkerPool()
{
    Clean();
}

Status UcpWorkerPool::Init()
{
    for (uint32_t i = 0; i < workerN_; i++) {
        std::shared_ptr<UcpWorker> ucpWorker = std::make_shared<UcpWorker>(context_, i);
        RETURN_IF_NOT_OK(ucpWorker->Init());
        localWorkerPool_.emplace(i, std::move(ucpWorker));
    }

    return Status::OK();
}

Status UcpWorkerPool::Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                            const std::string &remoteWorkerAddr, const std::string &ipAddr,
                            const uintptr_t localSegAddr, size_t localSegSize, uint64_t requestID,
                            std::shared_ptr<Event> event)
{
    UcpWorker *worker = GetOrSelSendWorker(ipAddr, requestID);
    if (worker == nullptr) {
        // no worker found and no worker created
        VLOG(ERROR) << FormatString("Communication with IP %s Failed", ipAddr);
        RETURN_STATUS(K_RDMA_ERROR, std::string("[UcpWorkerPool] Failed to obtain worker for communication."));
    }
    // this process is locked inside UcpWorker so no need to lock here
    return worker->Write(remoteRkey, remoteSegAddr, remoteWorkerAddr, ipAddr, localSegAddr, localSegSize, requestID,
                         std::move(event));
}

Status UcpWorkerPool::WriteN(const std::string &remoteRkey, uintptr_t remoteBaseAddr,
                             const std::string &remoteWorkerAddr, const std::string &ipAddr,
                             const std::vector<IovSegment> &segments, uint64_t requestID, std::shared_ptr<Event> event)
{
    UcpWorker *worker = GetOrSelSendWorker(ipAddr, requestID);
    if (worker == nullptr) {
        VLOG(ERROR) << FormatString("Communication with IP %s Failed", ipAddr);
        RETURN_STATUS(K_RDMA_ERROR, std::string("[UcpWorkerPool] Failed to obtain worker for communication."));
    }
    return worker->WriteN(remoteRkey, remoteBaseAddr, remoteWorkerAddr, ipAddr, segments, requestID, std::move(event));
}

std::string UcpWorkerPool::GetOrSelRecvWorkerAddr(const std::string &ipAddr)
{
    {
        std::shared_lock<std::shared_mutex> readLock(recvMapMutex_);
        // first check the Recv map to see if the ipAddr has appeared before
        // need to lock guard -- read-only
        auto it = localWorkerRecvMap_.find(ipAddr);
        if (it != localWorkerRecvMap_.end()) {
            return it->second;
        }
    }

    // nothing found, get a random one and return the worker address
    // need to lock guard -- write
    std::unique_lock<std::shared_mutex> writeLock(recvMapMutex_);

    auto it = localWorkerRecvMap_.find(ipAddr);
    if (it != localWorkerRecvMap_.end()) {
        return it->second;
    }

    if (localWorkerPool_.empty()) {
        LOG(ERROR) << "Failed to select recv worker for " << ipAddr << ": UCP worker pool is empty";
        return "";
    }

    const size_t workerCount = localWorkerPool_.size();
    const size_t workerIndex = roundRobin_.fetch_add(1, std::memory_order_relaxed) % workerCount;
    VLOG(1) << "Select new ucp worker " << workerIndex << " for " << ipAddr << " to recv";
    auto &worker = localWorkerPool_.at(workerIndex);
    const std::string &workerAddr = worker->GetLocalWorkerAddr();
    localWorkerRecvMap_.emplace(ipAddr, workerAddr);
    return workerAddr;
}

Status UcpWorkerPool::RemoveByIp(const std::string &ipAddr)
{
    // need to clean both recv and send map. First do recv

    {
        // first check if there is a worker attached to this IP
        std::unique_lock writeLock(recvMapMutex_);
        if (localWorkerRecvMap_.erase(ipAddr) <= 0) {
            LOG(INFO) << FormatString("Try to remove by IP but never received from %s", ipAddr);
        }
    }

    for (auto &[workerId, worker] : localWorkerPool_) {
        Status status = worker->RemoveEndpointByIp(ipAddr);
        if (status.IsError()) {
            LOG(WARNING) << FormatString("Try to remove by IP %s on worker %u but failed: %s", ipAddr, workerId,
                                         status.ToString());
        }
    }
    return Status::OK();
}

UcpWorker *UcpWorkerPool::GetOrSelSendWorker(const std::string &ipAddr, uint64_t requestID)
{
    (void)ipAddr;
    (void)requestID;
    if (localWorkerPool_.empty()) {
        return nullptr;
    }

    const size_t workerCount = localWorkerPool_.size();
    const size_t workerIndex = roundRobin_.fetch_add(1, std::memory_order_relaxed) % workerCount;
    auto worker = localWorkerPool_[workerIndex].get();
    return worker;
}

void UcpWorkerPool::Clean()
{
    // first clean the maps
    // UcpWorker instances will deconstruct properly by its deconstructor
    {
        std::unique_lock<std::shared_mutex> writeLock(recvMapMutex_);
        localWorkerRecvMap_.clear();
    }

    localWorkerPool_.clear();
}

}  // namespace datasystem

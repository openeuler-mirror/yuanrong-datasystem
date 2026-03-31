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

#include <thread>
#include <sstream>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/ucp_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

UcpWorkerPool::UcpWorkerPool(const ucp_context_h &ucpContext, UcpManager *manager, uint32_t workerN)
    : context_(ucpContext), manager_(manager), workerN_(workerN)
{
}

UcpWorkerPool::~UcpWorkerPool()
{
    Clean();
}

Status UcpWorkerPool::Init()
{
    PerfPoint point(PerfKey::RDMA_UCP_WORKER_POOL_INIT);
    for (uint32_t i = 0; i < workerN_; i++) {
        std::shared_ptr<UcpWorker> ucpWorker = std::make_shared<UcpWorker>(context_, manager_, i);
        RETURN_IF_NOT_OK(ucpWorker->Init());
        localWorkerPool_.emplace(i, std::move(ucpWorker));
    }

    point.Record();
    return Status::OK();
}

Status UcpWorkerPool::Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                            const std::string &remoteWorkerAddr, const std::string &ipAddr,
                            const uintptr_t localSegAddr, size_t localSegSize, uint64_t requestID)
{
    UcpWorker *worker = GetOrSelSendWorker(ipAddr);
    if (worker == nullptr) {
        // no worker found and no worker created
        VLOG(ERROR) << FormatString("Communication with IP %s Failed", ipAddr);
        RETURN_STATUS(K_RDMA_ERROR, std::string("[UcpWorkerPool] Failed to obtain worker for communication."));
    }
    // this process is locked inside UcpWorker so no need to lock here
    return worker->Write(remoteRkey, remoteSegAddr, remoteWorkerAddr, ipAddr, localSegAddr, localSegSize, requestID);
}

Status UcpWorkerPool::WriteN(const std::string &remoteRkey, uintptr_t remoteBaseAddr,
                             const std::string &remoteWorkerAddr, const std::string &ipAddr,
                             const std::vector<IovSegment> &segments, uint64_t requestID)
{
    UcpWorker *worker = GetOrSelSendWorker(ipAddr);
    if (worker == nullptr) {
        VLOG(ERROR) << FormatString("Communication with IP %s Failed", ipAddr);
        RETURN_STATUS(K_RDMA_ERROR, std::string("[UcpWorkerPool] Failed to obtain worker for communication."));
    }
    return worker->WriteN(remoteRkey, remoteBaseAddr, remoteWorkerAddr, ipAddr, segments, requestID);
}

std::string UcpWorkerPool::GetOrSelRecvWorkerAddr(const std::string &ipAddr)
{
    PerfPoint point(PerfKey::RDMA_UCP_WORKER_POOL_GET_RECV_WORKER_ADDR);
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

    LOG(INFO) << "Select new ucp worker " << roundRobin_ << " for " << ipAddr << " to recv";
    auto &worker = localWorkerPool_[roundRobin_];
    roundRobin_ = (roundRobin_ + 1) % localWorkerPool_.size();

    const std::string &workerAddr = worker->GetLocalWorkerAddr();
    localWorkerRecvMap_.emplace(ipAddr, workerAddr);
    point.Record();
    return workerAddr;
}

Status UcpWorkerPool::RemoveByIp(const std::string &ipAddr)
{
    // need to clean both recv and send map. First do recv

    PerfPoint point(PerfKey::RDMA_UCP_WORKER_POOL_RM_IP);
    std::stringstream errMsg;
    {
        // first check if there is a worker attached to this IP
        std::unique_lock writeLock(recvMapMutex_);
        if (localWorkerRecvMap_.erase(ipAddr) <= 0) {
            LOG(INFO) << FormatString("Try to remove by IP but never received from %s", ipAddr);
        }
    }

    {
        // then check for send. Need to go inside UcpWorker and clean ep
        std::unique_lock writeLock(sendMapMutex_);
        // get the remoteWorkerAddr
        auto it = localWorkerSendMap_.find(ipAddr);
        if (it != localWorkerSendMap_.end()) {
            Status status = it->second->RemoveEndpointByIp(ipAddr);
            localWorkerSendMap_.erase(it);
            if (status.IsError()) {
                LOG(WARNING) << FormatString("Try to remove by IP %s but failed: %s", ipAddr, status.ToString());
            }
        } else {
            LOG(INFO) << FormatString("Try to remove by IP but never sent to %s", ipAddr);
        }
    }
    point.Record();
    return Status::OK();
}

UcpWorker *UcpWorkerPool::GetOrSelSendWorker(const std::string &ipAddr)
{
    {
        std::shared_lock<std::shared_mutex> readLock(sendMapMutex_);
        // first check the Send map to see if the ipAddr has appeared before
        // need to lock guard -- read-only
        auto it = localWorkerSendMap_.find(ipAddr);
        if (it != localWorkerSendMap_.end()) {
            return it->second;
        }
    }

    // nothing found, need to get the NEXT worker and add to map
    // write lock to protect the map

    std::unique_lock<std::shared_mutex> writeLock(sendMapMutex_);

    auto it = localWorkerSendMap_.find(ipAddr);
    if (it != localWorkerSendMap_.end()) {
        return it->second;
    }
    LOG(INFO) << "Select new ucp worker " << roundRobin_ << " for " << ipAddr << " to send";
    auto worker = localWorkerPool_[roundRobin_].get();
    roundRobin_ = (roundRobin_ + 1) % localWorkerPool_.size();
    localWorkerSendMap_.emplace(ipAddr, worker);
    return worker;
}

void UcpWorkerPool::Clean()
{
    // first clean the maps
    // UcpWorker instances will deconstruct properly by its deconstructor
    {
        std::unique_lock<std::shared_mutex> writeLock(sendMapMutex_);
        localWorkerSendMap_.clear();
    }
    {
        std::unique_lock<std::shared_mutex> writeLock(recvMapMutex_);
        localWorkerRecvMap_.clear();
    }

    localWorkerPool_.clear();
}

}  // namespace datasystem
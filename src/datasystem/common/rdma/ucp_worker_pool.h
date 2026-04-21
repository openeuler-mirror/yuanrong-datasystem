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

#ifndef DATASYSTEM_COMMON_RDMA_UCP_WORKER_POOL_H
#define DATASYSTEM_COMMON_RDMA_UCP_WORKER_POOL_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/common/rdma/ucp_dlopen_util.h"
#include "datasystem/common/rdma/ucp_worker.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class UcpWorkerPool {
public:
    UcpWorkerPool() = default;

    explicit UcpWorkerPool(const ucp_context_h &ucpContext, uint32_t workerN);
    virtual ~UcpWorkerPool();

    /**
     * @brief initialize a UcpWorkerPool with workerN number of UcpWorkers
     * @return Status::OK() if successful, otherwise error messages
     */
    virtual Status Init();

    /**
     * @brief asynchronously write to a remote memory
     * @param remoteRkey the rkey passed from a remote server
     * @param remoteSegAddr the head pointer to the remote buffer that will be written into
     * @param remoteWorkerAddr the worker address of a remote worker used for communication
     * @param ipAddr the IP address of the remote server
     * @param localSegAddr pointer to the head of local memory containing the content to be sent
     * @param localSegSize length of the content to be sent
     * @param requestID request ID passed in by UcpManager to track progress
     * @return Status::OK() if successfully executed write, otherwise error message
     * Notice that this function does not guarantee the success of the actual RDMA put action. UcpManager
     * will actively check the log filled by CallBack function in UcpWorker to see if the put action is
     * really successful. However, if this function fails, the put action would have failed.
     */
    virtual Status Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                         const std::string &remoteWorkerAddr, const std::string &ipAddr, const uintptr_t localSegAddr,
                         size_t localSegSize, uint64_t requestID, std::shared_ptr<Event> event = nullptr);

    /**
     * @brief Asynchronously write multiple memory segments to a contiguous remote memory region using IOV mode.
     * @param remoteRkey The rkey passed from a remote server.
     * @param remoteBaseAddr Base address of the remote contiguous memory region.
     * @param remoteWorkerAddr The worker address of a remote worker used for communication.
     * @param ipAddr The IP address of the remote server.
     * @param segments Vector of local memory segments to transfer.
     * @param requestID Request ID passed in by UcpManager to track progress.
     * @return Status::OK() if successfully executed write, otherwise error message.
     */
    virtual Status WriteN(const std::string &remoteRkey, uintptr_t remoteBaseAddr, const std::string &remoteWorkerAddr,
                          const std::string &ipAddr, const std::vector<IovSegment> &segments, uint64_t requestID,
                          std::shared_ptr<Event> event = nullptr);

    /**
     * @brief Obtain the worker that previously talked with this IP, or assign a new one
     * @param ipAddr IP address of the remote server
     * @return local worker address to be sent to remote server in the form of a string
     */
    virtual std::string GetOrSelRecvWorkerAddr(const std::string &ipAddr);

    /**
     * @brief remove EP and map entries associated with a broken remote IP and worker address
     * @param ipAddr a reference to the remote IP address that is broken
     * @return Status::OK() if successful, otherwise error messages
     */
    virtual Status RemoveByIp(const std::string &ipAddr);

private:
    UcpWorker *GetOrSelSendWorker(const std::string &ipAddr, uint64_t requestID);

    void Clean();

    // env variables
    ucp_context_h context_;
    uint32_t workerN_;

    // all worker info. key: worker index; value: UcpWorker
    std::unordered_map<uint32_t, std::shared_ptr<UcpWorker>> localWorkerPool_;

    // worker for receiving. key: remote IP address; value: ucp worker address for receiving
    std::unordered_map<std::string, std::string> localWorkerRecvMap_;

    // round robin, will increment whenever used
    std::atomic<size_t> roundRobin_{ 0 };

    // locks
    std::shared_mutex recvMapMutex_;  // protect localWorkerRecvMap_
};
}  // namespace datasystem

#endif

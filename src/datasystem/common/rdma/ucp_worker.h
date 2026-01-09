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
 * Description: UcpWorker class that handles the RDMA put process and managed
 * by UcpWorkerPool. It handles the reuse and removal of resources to previously
 * connected nodes.
 */

#ifndef DATASYSTEM_COMMON_RDMA_UCP_WORKER_H
#define DATASYSTEM_COMMON_RDMA_UCP_WORKER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "ucp/api/ucp.h"

#include "datasystem/common/rdma/ucp_endpoint.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class UcpManager;

class UcpWorker {
public:
    UcpWorker() = default;

    explicit UcpWorker(const ucp_context_h &ucpContext, UcpManager *manager, const uint32_t workerId);
    virtual ~UcpWorker();

    /**
     * @brief initialize a UcpWorker instance
     * @return Status::OK() if successful, otherwise Status with error message.
     */
    virtual Status Init();

    /**
     * @brief Write the info from localSegment to remote Address
     * @param remoteRkey remote rkey in the form of a string
     * @param remoteSegment remote segment address
     * @param remoteWorker remote worker address in the form of a string
     * @param ipAddr IP address to the remote node. First will check if previously generated EP exists,
     *               otherwise generate a new one with the remote worker address and store by this IP.
     * @param localSegAddr local segment address (after considering all offsets)
     * @param localSegSize local segment size (after considering all offsets)
     * @param requestID request ID passed in by UcpManager to track progress
     * @return Status::OK if UCP worker initialized successful, else Status::Error
     * Description: This function will create a worker on the node based on context created by initUCPContextLocal.
     *   It will assign worker address to worker_addr_ and the address length to workerAddrLen_ for later use.
     */
    virtual Status Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                         const std::string &remoteWorkerAddr, const std::string &ipAddr, const uintptr_t localSegAddr,
                         size_t localSegSize, uint64_t requestID);

    /**
     * @brief remove ep tied to a remote worker
     * @param[in] ipAddr IP address of the remote server
     * @return Status::OK() if successful, otherwise Status with error message
     */
    virtual Status RemoveEndpointByIp(const std::string &ipAddr);

    /**
     * @brief return a worker address to be sent to remote for receiving
     * @return a worker address in the form of a string.
     */
    virtual const std::string &GetLocalWorkerAddr() const
    {
        return localWorkerAddrStr_;
    }

private:
    void StartProgressThread();
    void StopProgressThread();
    void ProgressLoop();

    /**
     * @brief bind remote endpoint to the ucp worker, stored in remoteEndpointMap_
     * @param[in] remoteWorkerAddr ucp endpoint ptr in the form of a string
     * @return a pointer to a UcpEndpoint instance.
     */
    std::shared_ptr<UcpEndpoint> GetOrCreateEndpoint(const std::string &ipAddr, const std::string &remoteWorkerAddr);

    /**
     * @brief asynchronous put events to log so manager can read the status of the put action
     */
    void Wait(void *request);
    void Clean();

    static void CallBack(void *request, ucs_status_t status, void *userData);
    struct CallbackContext {
        UcpWorker *worker;
        uint64_t request_id;
        void *put_request;

        CallbackContext(UcpWorker *w, uint64_t id, void *req)
            : worker(w), request_id(id), put_request(req)
        {
        }
    };

    // env variables
    ucp_context_h context_;
    UcpManager *manager_;

    // status variables
    uint32_t workerId_;
    std::string errorMsgHead_;
    std::unique_ptr<Thread> progressThread_{ nullptr };
    std::atomic<bool> running_{ false };

    // variables for endpoint creation
    ucp_worker_h worker_ = nullptr;

    // variable needed to send to remote
    ucp_address_t *localWorkerAddr_ = nullptr;
    std::string localWorkerAddrStr_;

    std::unordered_map<std::string, std::shared_ptr<UcpEndpoint>> remoteEndpointMap_;

    // locks
    std::shared_mutex mapLock_;  // protect Ep map
    std::mutex writeLock_;       // protect write method

    // Worker Progress max sleep time
    static constexpr int WORKER_PROGRESS_SLEEP_TIMEOUT_MS = 5;  // ms
};

}  // namespace datasystem

#endif
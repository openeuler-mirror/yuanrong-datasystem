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
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"
#include "datasystem/common/rdma/ucp_endpoint.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Structure representing a local memory segment for IOV transfer.
 */
struct IovSegment {
    uintptr_t localAddr;  // Local memory address
    size_t size;          // Size of the segment in bytes
};

class UcpWorker {
public:
    UcpWorker() = default;

    explicit UcpWorker(const ucp_context_h &ucpContext, const uint32_t workerId);
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
                         size_t localSegSize, uint64_t requestID, std::shared_ptr<Event> event = nullptr);

    /**
     * @brief Write multiple memory segments to a contiguous remote memory region using IOV mode.
     * @param remoteRkey Remote rkey in the form of a string.
     * @param remoteBaseAddr Base address of the remote contiguous memory region.
     * @param remoteWorkerAddr Remote worker address in the form of a string.
     * @param ipAddr IP address of the remote node.
     * @param segments Vector of local memory segments to transfer.
     * @param requestID Request ID passed in by UcpManager to track progress.
     * @return Status::OK if successful, otherwise Status with error message.
     */
    virtual Status WriteN(const std::string &remoteRkey, uintptr_t remoteBaseAddr, const std::string &remoteWorkerAddr,
                          const std::string &ipAddr, const std::vector<IovSegment> &segments, uint64_t requestID,
                          std::shared_ptr<Event> event = nullptr);

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
    void StartSubmitThread();
    void StopSubmitThread();
    void SubmitLoop();
    Status WriteDirect(const std::string &remoteRkey, uintptr_t remoteSegAddr, const std::string &remoteWorkerAddr,
                       const std::string &ipAddr, uintptr_t localSegAddr, size_t localSegSize, uint64_t requestID,
                       std::shared_ptr<Event> event);
    Status WriteNDirect(const std::string &remoteRkey, uintptr_t remoteBaseAddr, const std::string &remoteWorkerAddr,
                        const std::string &ipAddr, const std::vector<IovSegment> &segments, uint64_t requestID,
                        std::shared_ptr<Event> event);

    /**
     * @brief Prepare IOV buffer for ucp_put_nbx with IOV datatype.
     * @param segments Vector of local memory segments.
     * @return Pointer to allocated IOV buffer vector.
     */
    std::vector<ucp_dt_iov_t> *PrepareIovBuffer(const std::vector<IovSegment> &segments);

    /**
     * @brief bind remote endpoint to the ucp worker, stored in remoteEndpointMap_
     * @param[in] remoteWorkerAddr ucp endpoint ptr in the form of a string
     * @return a pointer to a UcpEndpoint instance.
     */
    std::shared_ptr<UcpEndpoint> GetOrCreateEndpoint(const std::string &ipAddr, const std::string &remoteWorkerAddr);

    void Clean();

    struct CallbackContext {
        UcpWorker *worker;
        ucp_ep_h ep;
        std::shared_ptr<UcpEndpoint> endpoint;
        uint64_t request_id;
        void *put_request;
        std::vector<ucp_dt_iov_t> *iov;
        std::shared_ptr<Event> event;
        uint64_t flush_start_ns;

        CallbackContext(UcpWorker *w, std::shared_ptr<UcpEndpoint> targetEndpoint, uint64_t id, void *req,
                        std::vector<ucp_dt_iov_t> *iovVec, std::shared_ptr<Event> callbackEvent,
                        uint64_t flushStartNs = 0)
            : worker(w),
              ep(targetEndpoint == nullptr ? nullptr : targetEndpoint->GetEp()),
              endpoint(std::move(targetEndpoint)),
              request_id(id),
              put_request(req),
              iov(iovVec),
              event(std::move(callbackEvent)),
              flush_start_ns(flushStartNs)
        {
        }
    };
    struct FlushBatchContext {
        UcpWorker *worker;
        std::vector<CallbackContext *> contexts;
        bool inflight{ false };
    };
    struct SubmitRequest {
        bool isIov{ false };
        std::string remoteRkey;
        uintptr_t remoteAddr{ 0 };
        std::string remoteWorkerAddr;
        std::string ipAddr;
        uintptr_t localSegAddr{ 0 };
        size_t localSegSize{ 0 };
        std::vector<IovSegment> segments;
        uint64_t requestID{ 0 };
        std::shared_ptr<Event> event;
        uint64_t enqueueNs{ 0 };
    };

    void EnqueueFlush(CallbackContext *ctx);
    Status EnqueueSubmit(std::shared_ptr<SubmitRequest> req);
    void FinishContext(CallbackContext *ctx, bool failed);
    void FinishBatch(FlushBatchContext *batchCtx, bool failed);
    static void FlushCallBack(void *request, ucs_status_t status, void *userData);

    // env variables
    ucp_context_h context_;

    // status variables
    uint32_t workerId_;
    std::string errorMsgHead_;
    std::unique_ptr<Thread> submitThread_{ nullptr };
    std::atomic<bool> submitRunning_{ false };

    // variables for endpoint creation
    ucp_worker_h worker_ = nullptr;

    // variable needed to send to remote
    ucp_address_t *localWorkerAddr_ = nullptr;
    std::string localWorkerAddrStr_;

    std::unordered_map<std::string, std::shared_ptr<UcpEndpoint>> remoteEndpointMap_;

    // locks
    std::shared_mutex mapLock_;  // protect Ep map
    std::deque<CallbackContext *> flushQueue_;
    std::mutex submitMutex_;
    std::condition_variable submitCv_;
    std::deque<std::shared_ptr<SubmitRequest>> submitQueue_;
    uint64_t outstandingFlushes_{ 0 };
};

}  // namespace datasystem

#endif

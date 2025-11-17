/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Defines the worker worker service processing main class.
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_WORKER_WORKER_SC_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_STREAM_CACHE_WORKER_WORKER_SC_SERVICE_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/buffer_pool.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/usage_monitor.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
struct RecvElementView : public BaseBufferData {
    std::string streamName_;
    RpcMessage recvBuffer_;  // Holding this buffer so that we free it later
    std::string workerAddr_;
    std::string workerInstanceId_;
    uint64_t seqNo_{ 0 };
    std::vector<size_t> sz_;
    std::vector<bool> headerBits_;
    std::unique_ptr<char[]> localBuf_;  // For decrypted data
    std::atomic<bool> decrypted_{ false };
    size_t totalLength_{ 0 };
    uint64_t idx_{ 0 };

    std::string StreamName() const override;
    std::string ProducerName() const override;
    std::string ProducerInstanceId() const override;
    uint64_t StreamHash() const override;
    Status ReleasePage() override;
    void *GetBufferPointer();
};

class WorkerWorkerSCServiceImpl : public WorkerWorkerSCService,
                                  public std::enable_shared_from_this<WorkerWorkerSCServiceImpl> {
public:
    WorkerWorkerSCServiceImpl(ClientWorkerSCServiceImpl *impl, std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerWorkerSCServiceImpl() override;

    /**
     * @brief Init Worker worker sc service.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Receive pushed elements and cursors from pub workers.
     * @param[in, out] serverApi The server reader writer session.
     * @return K_OK on success; the error code otherwise.
     */
    Status PushElementsCursors(std::shared_ptr<ServerUnaryWriterReader<PushRspPb, PushReqPb>> serverApi) override;

    /**
     * @brief Helper function to handle the pushed elements.
     * @param[in] req The request protobuf.
     * @param[in] payloads The actual data payloads.
     * @param[out] rsp The response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PushElementsCursorsHelper(PushReqPb &req, std::vector<RpcMessage> &payloads, PushRspPb &rsp);

    /**
     * @brief Receive pushed elements and cursors from pub workers, for shared page scenario.
     * @param[in, out] serverApi The server reader writer session.
     * @return K_OK on success; the error code otherwise.
     */
    Status PushSharedPageCursors(
        std::shared_ptr<ServerUnaryWriterReader<PushRspPb, SharedPagePushReqPb>> serverApi) override;

    Status ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr, std::list<BaseData> dataLst,
                              const std::string &streamName, const std::string &workerAddr);

    void PurgeBuffer(const std::shared_ptr<StreamManager> &streamManager);

    /**
     * @brief Remove the info of useless stream from BufferPool
     * @param keyName The stream name or page name.
     * @param sharedPageName The shared page name. Empty if the stream use exclusive page or the keyName is page.
     */
    void RemoveStream(const std::string &keyName, const std::string &sharedPageName);

    /**
     * @brief Get usage monitor.
     * @return The reference to the usage monitor.
     */
    UsageMonitor &GetUsageMonitor();

private:
    /**
     * @brief Batch Async flush entry.
     * @param[in] myId The num of Partitions.
     * @param[in] pendingFlushList Flush list for pending.
     * @return Status of the call.
     */
    Status BatchAsyncFlushEntry(int myId, const PendingFlushList &pendingFlushList);

    /**
     * @brief Async flush entry.
     * @param[in] baseBufferData The entry to be flush.
     * @param[in] streamName The stream name.
     * @param[in] sendWorkerAddr The send worker address.
     * @param[in] isBlocked Whether the remote producer is blocked.
     * @return Status of the call.
     */
    Status ProcessRecvElementView(std::shared_ptr<BaseBufferData> &baseBufferData, const std::string &streamName,
                                  const std::string &sendWorkerAddr, bool &isBlocked);

    /**
     * @brief Check the stream state.
     * @param[in] streamName The stream name.
     * @param[in] accessor The StreamManagerMap accessor.
     * @param[in] mgr The instance of StreamManager.
     * @return Status of the call.
     */
    Status CheckStreamState(const std::string &streamName, StreamManagerMap::const_accessor &accessor,
                            std::shared_ptr<StreamManager> &mgr);

    /**
     * @brief Parse the data from payload.
     * @param[in] pushReqPb PushReqPb message.
     * @param[in] payloads Payloads data list.
     * @param[in] streamManager StreamManager pointer.
     * @param[in] workerAddr Worker address.
     * @param[out] pushRspPb PushRspPb message.
     * @param[out] flushList Flush list for BaseBufferData.
     * @param[out] totalSize Total Size of element.
     * @return Status of the call.
     */
    Status ParsePushData(const PushReqPb &pushReqPb, std::vector<RpcMessage> &payloads,
                         std::shared_ptr<StreamManager> &streamManager, const std::string &workerAddr,
                         PushRspPb &pushRspPb, std::vector<std::shared_ptr<BaseBufferData>> &flushList);

    /**
     * @brief Checks all error and returns the first one.
     * @param[in] rc vector of errors for each producer
     * @return status of the call
     */
    Status ReturnFirstErrorStatus(const std::vector<Status> &rc);

    struct PushSharedPageTuple {
        PushReqPb req_;
        std::vector<RpcMessage> payload_;
        std::vector<uint64_t> index_;
    };

    std::shared_ptr<AkSkManager> akSkManager_;
    ClientWorkerSCServiceImpl *clientWorkerScService_;
    std::unique_ptr<BufferPool> dataMap_;
    UsageMonitor usageMonitor_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_STREAM_CACHE_WORKER_WORKER_SC_SERVICE_IMPL_H

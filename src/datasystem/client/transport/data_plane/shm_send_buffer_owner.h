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

/** Description: Owns the write buffer created by routed Create/Publish.
 * Releases the worker reference through the bound WorkerRpcClient (routed worker),
 * not LOCAL_WORKER. Used by both SHM (ShmTransporter) and UB (UbTransporter).
 * For SHM, an optional lifecycle handle (mmapEntry / UB pool handle) keeps the
 * mapping alive until the queued DecreaseReference completes. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_SEND_BUFFER_OWNER_H
#define DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_SEND_BUFFER_OWNER_H

#include <atomic>
#include <functional>
#include <memory>

#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/object_cache/ireceive_buffer_owner.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace client {

/** @brief Send-buffer owner that releases the target worker reference through the bound
 *  WorkerRpcClient for routed Create-allocated write buffers (SHM and UB). */
class ShmSendBufferOwner final : public IReceiveBufferOwner {
public:
    /** @param rpcClient The routed worker's RPC client (releases shmId on the correct worker).
     *  @param shmId The shmId allocated by Create on the routed worker.
     *  @param context Request context (clientId/token/tenantId for the DecreaseReference RPC).
     *  @param releasePool Async pool for retry; empty = synchronous fallback.
     *  @param lifecycleHandle Optional shared_ptr that keeps a mapping/handle alive until Release
     *         completes (e.g. mmapEntry for SHM, UB pool handle for UB).
     *  @param livenessCheck Optional function for data-plane liveness gate (e.g. ShmSession::IsAlive
     *         for SHM fd-passing). When set, CheckAlive calls both rpcClient->IsAlive() and this
     *         function. When empty (UB path), only rpcClient->IsAlive() is checked. */
    ShmSendBufferOwner(std::shared_ptr<WorkerRpcClient> rpcClient, ShmKey shmId,
                       TransportRequestContext context, std::weak_ptr<ThreadPool> releasePool,
                       std::shared_ptr<void> lifecycleHandle = nullptr,
                       std::function<bool()> livenessCheck = nullptr);
    ~ShmSendBufferOwner() override;

    void Release() override;
    bool ManagesWorkerReference() const override;
    Status CheckAlive() const override;

private:
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    ShmKey shmId_;
    TransportRequestContext context_;
    std::weak_ptr<ThreadPool> releasePool_;
    std::shared_ptr<void> lifecycleHandle_;  // keeps SHM mmap or UB handle alive until release
    std::function<bool()> livenessCheck_;   // optional data-plane liveness gate (e.g. ShmSession::IsAlive)
    std::atomic<bool> released_{ false };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_SEND_BUFFER_OWNER_H

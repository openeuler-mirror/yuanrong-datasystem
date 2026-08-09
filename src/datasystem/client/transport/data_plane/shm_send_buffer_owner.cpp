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
 * Releases the worker reference through the bound WorkerRpcClient (routed worker).
 * Used by both SHM (ShmTransporter via ShmSession::MmapWriteRegion) and UB (UbTransporter::Create). */
#include "datasystem/client/transport/data_plane/shm_send_buffer_owner.h"

#include <chrono>
#include <thread>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
constexpr int64_t WRITE_REFERENCE_RELEASE_TIMEOUT_MS = 1000;
}  // namespace

ShmSendBufferOwner::ShmSendBufferOwner(std::shared_ptr<WorkerRpcClient> rpcClient, ShmKey shmId,
                                        TransportRequestContext context, std::weak_ptr<ThreadPool> releasePool,
                                        std::shared_ptr<void> lifecycleHandle,
                                        std::function<bool()> livenessCheck)
    : rpcClient_(std::move(rpcClient)),
      shmId_(shmId),
      context_(std::move(context)),
      releasePool_(std::move(releasePool)),
      lifecycleHandle_(std::move(lifecycleHandle)),
      livenessCheck_(std::move(livenessCheck))
{
}

ShmSendBufferOwner::~ShmSendBufferOwner()
{
    Release();
}

void ShmSendBufferOwner::Release()
{
    if (released_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto rpcClient = rpcClient_;
    if (rpcClient == nullptr || !rpcClient->IsAlive()) {
        // RPC client teardown delegates remaining references to worker client-lost cleanup.
        return;
    }
    auto releasePool = releasePool_.lock();
    if (releasePool == nullptr) {
        return;
    }
    try {
        releasePool->Execute([rpcClient = std::move(rpcClient), context = context_, shmId = shmId_,
                             handle = lifecycleHandle_]() {
            // Retry with backoff (mirrors TransportLayer::InvokeReleaseWithRetry): a single transient
            // failure must not drop the release (region would leak until client-lost). On exhaustion, log
            // and leave it to client-lost.
            constexpr int64_t backoffMs[] = { 0, 100, 400 };
            Status rc;
            for (size_t attempt = 0; attempt < sizeof(backoffMs) / sizeof(backoffMs[0]); ++attempt) {
                if (rpcClient == nullptr || !rpcClient->IsAlive()) {
                    return;
                }
                if (backoffMs[attempt] > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs[attempt]));
                }
                ApiDeadlineGuard deadlineGuard(WRITE_REFERENCE_RELEASE_TIMEOUT_MS);
                rc = rpcClient->InvokeDecreaseReference(context, shmId);
                if (rc.IsOk()) {
                    return;
                }
                if (IsNonRetryableRpcError(rc)) {
                    return;
                }
            }
            LOG(WARNING) << "WorkerOCService DecreaseReference failed for routed write buffer after "
                         << (sizeof(backoffMs) / sizeof(backoffMs[0])) << " attempts: " << rc.ToString()
                         << "; region will be reclaimed by worker client-lost";
        });
    } catch (const std::exception &e) {
        LOG(WARNING) << "Submit routed write reference release failed: " << e.what();
    }
}

bool ShmSendBufferOwner::ManagesWorkerReference() const
{
    return true;
}

Status ShmSendBufferOwner::CheckAlive() const
{
    if (rpcClient_ == nullptr || !rpcClient_->IsAlive()) {
        return Status(K_BUFFER_DEPRECATED, "Routed write RPC client is no longer alive");
    }
    // SHM path: also gate on the data-plane fd session liveness (e.g. ShmSession::IsAlive which
    // checks alive_ + fdChannel_). UB path has no livenessCheck_ (nullptr) — UB Set uses
    // conn_->IsAlive() independently, so owner CheckAlive only needs the RPC client.
    if (livenessCheck_ && !livenessCheck_()) {
        return Status(K_BUFFER_DEPRECATED, "Routed shared-memory write session is no longer alive");
    }
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem

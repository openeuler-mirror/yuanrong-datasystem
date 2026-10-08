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
#include "datasystem/client/object_cache/transport/data_plane/shm_send_buffer_owner.h"

#include <chrono>
#include <thread>

#include "datasystem/common/metrics/kv_metrics.h"
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
                                       std::function<bool()> livenessCheck, bool releaseUbHandleBeforeAsync)
    : rpcClient_(std::move(rpcClient)),
      shmId_(shmId),
      context_(std::move(context)),
      releasePool_(std::move(releasePool)),
      lifecycleHandle_(std::move(lifecycleHandle)),
      livenessCheck_(std::move(livenessCheck)),
      releaseUbHandleBeforeAsync_(releaseUbHandleBeforeAsync)
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
    const bool holding = holding_.load(std::memory_order_relaxed);
    const bool holdingMetricCounted = holdingMetricCounted_.exchange(false, std::memory_order_relaxed);
    auto finishHolding = [holdingMetricCounted]() {
        if (holdingMetricCounted) {
            metrics::AddClientUbHoldingCount(-1);
        }
    };
    auto rpcClient = rpcClient_;
    if (rpcClient == nullptr || !rpcClient->IsAlive()) {
        // RPC client teardown delegates remaining references to worker client-lost cleanup.
        finishHolding();
        return;
    }
    auto releasePool = releasePool_.lock();
    if (releasePool == nullptr) {
        finishHolding();
        return;
    }
    bool taskEnqueued = false;
    bool countUbReleasing = false;
    try {
        // Every queued UB DecreaseReference is releasing, including delayed release. Local-handle
        // return eligibility is independent of the asynchronous RPC task's lifecycle.
        countUbReleasing = releaseUbHandleBeforeAsync_ && metrics::IsClientUbLifecycleMetricsEnabled();
        const bool releaseUbHandle = releaseUbHandleBeforeAsync_ && !holding;
        if (releaseUbHandle) {
            // The URMA write has a final result. Do not let DecreaseReference queueing retain a local UB slot.
            lifecycleHandle_.reset();
        }
        if (countUbReleasing) {
            metrics::AddClientUbReleasingCount(1);
        }
        // The ObjectBuffer owner stays alive through Set, so an ambiguous write is marked before Release snapshots it.
        // Capture by value because the asynchronous task can outlive this owner.
        releasePool->ExecuteWithEnqueueStatus(
            taskEnqueued, [rpcClient = std::move(rpcClient), context = context_, shmId = shmId_,
                           handle = releaseUbHandle ? std::shared_ptr<void>{} : lifecycleHandle_,
                           delayRelease = delayRelease_.load(std::memory_order_acquire), holdingMetricCounted,
                           countUbReleasing]() {
                auto finish = [holdingMetricCounted, countUbReleasing]() {
                    if (holdingMetricCounted) {
                        metrics::AddClientUbHoldingCount(-1);
                    }
                    if (countUbReleasing) {
                        metrics::AddClientUbReleasingCount(-1);
                    }
                };
                // Retry with backoff (mirrors TransportLayer::InvokeReleaseWithRetry): a single transient
                // failure must not drop the release (region would leak until client-lost). On exhaustion, log
                // and leave it to client-lost.
                constexpr int64_t backoffMs[] = { 0, 100, 400 };
                Status rc;
                for (size_t attempt = 0; attempt < sizeof(backoffMs) / sizeof(backoffMs[0]); ++attempt) {
                    if (rpcClient == nullptr || !rpcClient->IsAlive()) {
                        finish();
                        return;
                    }
                    if (backoffMs[attempt] > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs[attempt]));
                    }
                    ApiDeadlineGuard deadlineGuard(WRITE_REFERENCE_RELEASE_TIMEOUT_MS);
                    rc = rpcClient->InvokeDecreaseReference(context, shmId, delayRelease);
                    if (rc.IsOk()) {
                        finish();
                        return;
                    }
                    if (IsNonRetryableRpcError(rc)) {
                        finish();
                        return;
                    }
                }
                LOG(WARNING) << "WorkerOCService DecreaseReference failed for routed write buffer after "
                             << (sizeof(backoffMs) / sizeof(backoffMs[0])) << " attempts: " << rc.ToString()
                             << "; region will be reclaimed by worker client-lost";
                finish();
            });
    } catch (const std::exception &e) {
        // Lazy worker creation can fail after taskQ_ accepts the task. A queued task owns its
        // cleanup, so only undo metrics when submission was rejected.
        if (!taskEnqueued && countUbReleasing) {
            metrics::AddClientUbReleasingCount(-1);
        }
        if (!taskEnqueued) {
            finishHolding();
        }
        LOG(WARNING) << "Submit routed write reference release "
                     << (taskEnqueued ? "queued but worker start failed: " : "failed: ") << e.what();
    }
}

void ShmSendBufferOwner::MarkDelayRelease()
{
    delayRelease_.store(true, std::memory_order_release);
    if (releaseUbHandleBeforeAsync_ && !holding_.exchange(true, std::memory_order_relaxed)
        && metrics::IsClientUbLifecycleMetricsEnabled()) {
        holdingMetricCounted_.store(true, std::memory_order_relaxed);
        metrics::AddClientUbHoldingCount(1);
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

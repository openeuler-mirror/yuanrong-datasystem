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

/** Description: Owns the ShmSession-backed write buffer created by routed Create/Publish. */
#include "datasystem/client/transport/data_plane/shm_send_buffer_owner.h"

#include <chrono>
#include <thread>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
constexpr int64_t SHM_WRITE_REFERENCE_RELEASE_TIMEOUT_MS = 1000;
}  // namespace

ShmSendBufferOwner::ShmSendBufferOwner(std::shared_ptr<ShmSession> session,
                                       std::shared_ptr<IMmapTableEntry> mmapEntry, ShmKey shmId,
                                       TransportRequestContext context, std::weak_ptr<ThreadPool> releasePool)
    : session_(std::move(session)),
      mmapEntry_(std::move(mmapEntry)),
      shmId_(shmId),
      context_(std::move(context)),
      releasePool_(std::move(releasePool))
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
    auto session = session_;
    if (!session->IsAlive()) {
        // Session teardown delegates all remaining references to Worker client-lost cleanup.
        // Avoid one queued task and warning per stale write buffer.
        return;
    }
    auto releasePool = releasePool_.lock();
    if (releasePool == nullptr) {
        return;  // pool gone; client-lost reclaims. Do not close the shared session.
    }
    try {
        releasePool->Execute([session = std::move(session), context = context_, shmId = shmId_]() {
            // Retry with backoff (mirrors TransportLayer::InvokeReleaseWithRetry): a single transient
            // failure must not drop the release (region would leak until client-lost). On exhaustion, log
            // and leave it to client-lost — do NOT Close the session, which would invalidate every other
            // in-flight release on the same endpoint.
            constexpr int64_t backoffMs[] = { 0, 100, 400 };
            Status rc;
            for (size_t attempt = 0; attempt < sizeof(backoffMs) / sizeof(backoffMs[0]); ++attempt) {
                if (!session->IsAlive()) {
                    return;
                }
                if (backoffMs[attempt] > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs[attempt]));
                }
                ApiDeadlineGuard deadlineGuard(SHM_WRITE_REFERENCE_RELEASE_TIMEOUT_MS);
                rc = session->DecreaseReferenceByRequestClient(context, shmId);
                if (rc.IsOk()) {
                    return;
                }
            }
            LOG(WARNING) << "WorkerOCService DecreaseReference failed for routed SHM write buffer after "
                         << (sizeof(backoffMs) / sizeof(backoffMs[0])) << " attempts: " << rc.ToString()
                         << "; region will be reclaimed by worker client-lost";
        });
    } catch (const std::exception &e) {
        LOG(WARNING) << "Submit routed SHM write reference release failed: " << e.what();
    }
}

bool ShmSendBufferOwner::ManagesWorkerReference() const
{
    return true;
}

Status ShmSendBufferOwner::CheckAlive() const
{
    return session_ != nullptr && session_->IsAlive()
               ? Status::OK()
               : Status(K_BUFFER_DEPRECATED, "Routed shared-memory write session is no longer alive");
}

}  // namespace client
}  // namespace datasystem

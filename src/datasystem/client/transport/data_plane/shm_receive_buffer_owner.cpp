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

/** Description: Owns the ShmSession-backed receive buffer released to the data worker. */
#include "datasystem/client/transport/data_plane/shm_receive_buffer_owner.h"

#include "datasystem/client/transport/data_plane/shm_connection.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
constexpr int64_t SHM_REFERENCE_RELEASE_TIMEOUT_MS = 1000;
}  // namespace

ShmReceiveBufferOwner::ShmReceiveBufferOwner(std::shared_ptr<ShmSession> session,
                                             std::shared_ptr<IMmapTableEntry> mmapEntry, ShmKey shmId,
                                             std::shared_ptr<const TransportReadContext> context,
                                             std::weak_ptr<ThreadPool> releasePool)
    : session_(std::move(session)),
      mmapEntry_(std::move(mmapEntry)),
      shmId_(shmId),
      context_(std::move(context)),
      releasePool_(std::move(releasePool))
{
}

ShmReceiveBufferOwner::~ShmReceiveBufferOwner()
{
    Release();
}

void ShmReceiveBufferOwner::Release()
{
    if (released_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto session = session_;
    if (!session->IsAlive()) {
        // Session teardown delegates all remaining references to Worker client-lost cleanup.
        // Avoid one queued task and warning per stale Buffer.
        return;
    }
    auto releasePool = releasePool_.lock();
    if (releasePool == nullptr) {
        session->Close(false);
        return;
    }
    try {
        releasePool->Execute([session = std::move(session), context = context_, shmId = shmId_]() {
            if (!session->IsAlive()) {
                return;
            }
            ApiDeadlineGuard deadlineGuard(SHM_REFERENCE_RELEASE_TIMEOUT_MS);
            Status rc = session->DecreaseReference(context->requestContext, shmId);
            if (rc.IsError()) {
                LOG(WARNING) << "WorkerOCService DecreaseReference failed for routed SHM buffer: " << rc.ToString();
                session->Close(false);
            }
        });
    } catch (const std::exception &e) {
        LOG(WARNING) << "Submit routed SHM reference release failed: " << e.what();
        session_->Close(false);
    }
}

bool ShmReceiveBufferOwner::ManagesWorkerReference() const
{
    return true;
}

Status ShmReceiveBufferOwner::CheckAlive() const
{
    return session_ != nullptr && session_->IsAlive()
               ? Status::OK()
               : Status(K_BUFFER_DEPRECATED, "Routed shared-memory session is no longer alive");
}

}  // namespace client
}  // namespace datasystem

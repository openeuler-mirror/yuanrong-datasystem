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
 * Description: SharedPageQueue
 */

#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DECLARE_string(sc_encrypt_secret_key);
DS_DECLARE_string(encrypt_kit);

namespace {
static bool ValidateSharedPageSize(const char *flagname, uint32_t value)
{
    const int32_t minValue = 1;
    const int32_t maxValue = 16;
    if ((value < minValue) || (value > maxValue)) {
        LOG(ERROR) << "The " << flagname << " must be between " << minValue << " and " << maxValue << ".";
        return false;
    }
    return true;
}
}  // namespace

DS_DEFINE_uint32(sc_shared_page_size_mb, 4, "the shared page size");
DS_DEFINE_validator(sc_shared_page_size_mb, &ValidateSharedPageSize);

namespace datasystem {
namespace worker {
namespace stream_cache {

SharedPageQueue::SharedPageQueue(std::string tenantId, HostPort remoteWorker, int partId,
                                 std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager,
                                 ClientWorkerSCServiceImpl *scSvc)
    : tenantId_(std::move(tenantId)),
      remoteWorker_(std::move(remoteWorker)),
      scAllocateManager_(scAllocateManager),
      scSvc_(scSvc)
{
    isSharedPage_ = true;
    if (tenantId_.empty()) {
        pageQueueId_ = FormatString("%s-%d", remoteWorker_.ToString(), partId);
    } else {
        pageQueueId_ = FormatString("%s-%s-%d", remoteWorker_.ToString(), tenantId_, partId);
    }
}

const std::string &SharedPageQueue::GetPageQueueId() const
{
    return pageQueueId_;
}

std::string SharedPageQueue::LogPrefix() const
{
    return FormatString("SPG:%s", GetPageQueueId());
}

size_t SharedPageQueue::GetPageSize() const
{
    return FLAGS_sc_shared_page_size_mb * MB_TO_BYTES;
}

Status SharedPageQueue::CheckHadEnoughMem(uint64_t memSize)
{
    (void)memSize;
    return Status::OK();
}

Status SharedPageQueue::UpdateLocalCursorLastDataPage(const ShmView &shmView)
{
    ReadLockHelper xlock(STREAM_COMMON_LOCK_ARGS(lastPageRefMutex_));
    if (lastPageRefShmViewImpl_ != nullptr) {
        WARN_IF_ERROR(lastPageRefShmViewImpl_->SetView(shmView, false, DEFAULT_TIMEOUT_MS),
                      FormatString("[%s] UpdateLocalCursorLastDataPage error", LogPrefix()));
    } else {
        LOG(WARNING) << LogPrefix() << " lastPageRefShmViewImpl_ not init!";
    }
    return Status::OK();
}

Status SharedPageQueue::AllocateMemoryImpl(size_t memSizeNeeded, ShmUnit &shmUnit, bool retryOnOOM)
{
    return scAllocateManager_->AllocateMemoryForStream(tenantId_, "", memSizeNeeded, true, shmUnit, retryOnOOM);
}

Status SharedPageQueue::AfterAck()
{
    return Status::OK();
}

Status SharedPageQueue::RemoteAck()
{
    auto lastAppendCursor = GetLastAppendCursor();
    uint64_t newAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
    RETURN_IF_NOT_OK(Ack(newAckCursor));
    return Status::OK();
}

uint64_t SharedPageQueue::UpdateLastAckCursorUnlocked(uint64_t minSubsAckCursor)
{
    bool success = false;
    do {
        uint64_t val = lastAckCursor_.load();
        // Go through all remote consumers. We may in the process of sending elements
        // to the remote worker.
        auto remoteWorkerManager = GetRemoteWorkerManager();
        auto remoteAckCursor = remoteWorkerManager->GetLastAckCursor(GetPageQueueId());
        minSubsAckCursor = std::min(minSubsAckCursor, remoteAckCursor);
        if (minSubsAckCursor > val) {
            INJECT_POINT_NO_RETURN("UpdateLastAckCursorUnlocked.sleep");
            success = lastAckCursor_.compare_exchange_strong(val, minSubsAckCursor);
            if (success) {
                LOG(INFO) << FormatString("[%s] The last ack of stream update from %zu to %zu", LogPrefix(), val,
                                          minSubsAckCursor);
                return minSubsAckCursor;
            }
        } else {
            return minSubsAckCursor;
        }
    } while (true);
}

bool SharedPageQueue::IsEncryptStream(const std::string &streamName) const
{
    StreamManagerMap::const_accessor accessor;
    Status rc = scSvc_->GetStreamManager(streamName, accessor);
    if (rc.IsError()) {
        return false;
    }
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    StreamFields streamFields;
    streamMgr->GetStreamFields(streamFields);
    return streamFields.encryptStream_ && !FLAGS_sc_encrypt_secret_key.empty()
           && FLAGS_encrypt_kit != ENCRYPT_KIT_PLAINTEXT;
}

std::string SharedPageQueue::GetStreamName() const
{
    return GetPageQueueId();
}

Status SharedPageQueue::GetOrCreateLastPageRef(ShmView &lastPageRefShmView,
                                               std::shared_ptr<SharedMemViewImpl> &lastPageRefShmViewImpl)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(lastPageRefMutex_));
    if (lastPageRefShmUnit_ == nullptr) {
        const size_t lastPageRefSize = sizeof(SharedMemView);
        auto shmUnit = std::make_unique<ShmUnit>();
        shmUnit->SetHardFreeMemory();
        RETURN_IF_NOT_OK(shmUnit->AllocateMemory(tenantId_, lastPageRefSize, false, ServiceType::STREAM));
        auto rc = memset_s(shmUnit->GetPointer(), lastPageRefSize, 0, lastPageRefSize);
        CHECK_FAIL_RETURN_STATUS(rc == 0, K_RUNTIME_ERROR,
                                 FormatString("[%s] Memset to 0 results in errno %d", LogPrefix(), rc));

        // The lock id of worker is 0.
        auto lastPageRefShmViewImpl =
            std::make_shared<SharedMemViewImpl>(shmUnit->GetPointer(), lastPageRefSize, WORKER_LOCK_ID);
        RETURN_IF_NOT_OK(lastPageRefShmViewImpl->Init(false));
        lastPageRefShmUnit_ = std::move(shmUnit);
        lastPageRefShmViewImpl_ = std::move(lastPageRefShmViewImpl);
        lastPageRefShmViewImpl_->SetView(ShmView{}, false, std::numeric_limits<uint64_t>::max());
    }
    lastPageRefShmView = lastPageRefShmUnit_->GetShmView();
    lastPageRefShmViewImpl = lastPageRefShmViewImpl_;
    return Status::OK();
}

std::shared_ptr<PageQueueBase> SharedPageQueue::SharedFromThis()
{
    return std::static_pointer_cast<PageQueueBase>(shared_from_this());
}

RemoteWorkerManager *SharedPageQueue::GetRemoteWorkerManager() const
{
    return scSvc_->GetRemoteWorkerManager();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

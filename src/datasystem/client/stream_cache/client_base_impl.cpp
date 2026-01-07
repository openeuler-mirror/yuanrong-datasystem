/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Implement stream cache base class for producer and consumer.
 */

#include "datasystem/client/stream_cache/client_base_impl.h"

#include "datasystem/client/stream_cache/producer_consumer_worker_api.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace client {
namespace stream_cache {
ClientBaseImpl::ClientBaseImpl(std::string streamName, std::string tenantId,
                               std::shared_ptr<ProducerConsumerWorkerApi> workerApi,
                               std::shared_ptr<StreamClientImpl> client, MmapManager *mmapManager,
                               std::shared_ptr<client::ListenWorker> listenWorker)
    : streamName_(std::move(streamName)),
      client_(std::move(client)),
      workerApi_(std::move(workerApi)),
      mmapManager_(mmapManager),
      listenWorker_(std::move(listenWorker)),
      lockId_(0),
      state_(State::NORMAL),
      tenantId_(std::move(tenantId))
{
}

ClientBaseImpl::~ClientBaseImpl() = default;

Status ClientBaseImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(workArea_ != ShmView(), K_RUNTIME_ERROR, "ShmView not initialized");
    lockId_ = workerApi_->GetLockId();
    // Set up the shared memory communication area
    auto shmUnitInfo = std::make_shared<ShmUnitInfo>(workArea_.fd, workArea_.mmapSz);
    RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd(tenantId_, shmUnitInfo));
    INJECT_POINT("ClientBaseImpl.init_fail_before_cursor");
    cursor_ = std::make_unique<Cursor>(static_cast<uint8_t *>(shmUnitInfo->GetPointer()) + workArea_.off, workArea_.sz,
                                       lockId_);
    RETURN_IF_NOT_OK(cursor_->Init(mmapManager_->GetMmapEntryByFd(shmUnitInfo->fd)));
    // If the worker is not down level, write something into the eye catcher area so that the worker
    // can also know our compatibility
    if (WorkAreaIsV2()) {
        cursor_->SetClientVersion(Cursor::K_CURSOR_SIZE_V2);
        workerVersion_ = cursor_->GetWorkerVersion();
    }
    return Status::OK();
}

const std::string &ClientBaseImpl::GetStreamName()
{
    return streamName_;
}

void ClientBaseImpl::SetInactive()
{
    {
        std::unique_lock<std::mutex> xlock(recvFdsMutex_);
        recvFds_.clear();
    }
    LOG_IF_ERROR(ChangeState(State::CLOSE), "SetInactive");
}

bool ClientBaseImpl::IsActive() const
{
    return state_ == State::NORMAL;
}

Status ClientBaseImpl::CheckState() const
{
    return listenWorker_->CheckWorkerAvailable();
}

Status ClientBaseImpl::CheckNormalState() const
{
    if (client_) {
        RETURN_IF_NOT_OK(client_->CheckWorkerLost());
    } else {
        return Status(K_RUNTIME_ERROR, "Client must not be null to do operations on consumer");
    }
    if (state_ == State::CLOSE) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_SC_ALREADY_CLOSED,
            FormatString("[%s] has been closed or inactive, please do not operate it", LogPrefix()));
    } else if (state_ == State::RESET) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_SC_STREAM_IN_RESET_STATE,
            FormatString("[%s] in Reset state, please do not operate it until Resume() is called", LogPrefix()));
    }
    return CheckState();
}

Status ClientBaseImpl::ChangeState(State newState)
{
    RETURN_OK_IF_TRUE(state_ == newState);  // no-op if the state is the same as before
    if (state_ == State::CLOSE) {
        RETURN_STATUS(K_SC_ALREADY_CLOSED,
                      FormatString("[%s] has been closed or inactive, please do not operate it", LogPrefix()));
    }
    state_ = newState;
    return Status::OK();
}

bool ClientBaseImpl::CheckStreamNameAndTenantId(const std::string &streamName, const std::string &tenantId)
{
    return GetStreamName() == streamName && GetTenantId() == tenantId;
}

Status ClientBaseImpl::GetShmInfo(const ShmView &shmView, std::shared_ptr<ShmUnitInfo> &out,
                                  std::shared_ptr<client::IMmapTableEntry> &mmapEntry)
{
    // Do a preliminary check if the fd makes any sense. There is no reason a file descriptor
    // can grow beyond the size of an unsigned short.
    if (static_cast<uint64_t>(shmView.fd) > std::numeric_limits<uint16_t>::max()) {
        RETURN_STATUS(K_OUT_OF_RANGE, FormatString("fd out of range. ShmView %s", shmView.ToStr()));
    }
    std::unique_lock<std::mutex> xlock(recvFdsMutex_);
    auto it = recvFds_.find(shmView.fd);
    if (it == recvFds_.end()) {
        auto pageUnit = std::make_shared<ShmUnitInfo>(shmView.fd, shmView.mmapSz);
        // Also return OUT_OF_RANGE to the caller that the given shmView is stale.
        auto rc = mmapManager_->LookupUnitsAndMmapFd(tenantId_, pageUnit);
        if (rc.IsError()) {
            RETURN_STATUS(K_OUT_OF_RANGE, FormatString("mmap error %s. ShmView %s", rc.GetMsg(), shmView.ToStr()));
        }
        bool success;
        std::tie(it, success) = recvFds_.emplace(shmView.fd, pageUnit);
        CHECK_FAIL_RETURN_STATUS(success, K_RUNTIME_ERROR,
                                 FormatString("Fail to insert ShmView [%s] into the map", shmView.ToStr()));
    }
    auto &pageUnit = it->second;
    out = std::make_shared<ShmUnitInfo>();
    out->pointer = pageUnit->pointer;
    out->fd = pageUnit->fd;
    out->mmapSize = pageUnit->mmapSize;
    out->size = shmView.sz;
    out->offset = shmView.off;
    mmapEntry = mmapManager_->GetMmapEntryByFd(pageUnit->fd);
    return Status::OK();
}

Status ClientBaseImpl::CheckAndSetInUse()
{
    bool expected = false;
    if (inUse_.compare_exchange_strong(expected, true)) {
        INJECT_POINT("CheckAndSetInUse.success.sleep");
        return Status::OK();
    }
    return Status(
        K_SC_STREAM_IN_USE,
        FormatString(
            "[%s] Another thread is using the producer/consumer, producer/consumer does not support multithreading.",
            LogPrefix()));
}

void ClientBaseImpl::UnsetInUse()
{
    bool expected = true;
    if (!inUse_.compare_exchange_strong(expected, false)) {
        // Sanity check failed.
        LOG(ERROR) << FormatString(
            "[%s] Runtime error: producer/consumer thread safety is not under protection by internal logic.",
            LogPrefix());
    }
}

bool ClientBaseImpl::WorkAreaIsV2() const
{
    bool isV2 = workArea_.sz == Cursor::K_CURSOR_SIZE_V2;
    INJECT_POINT_NO_RETURN("ClientBaseImpl.force_downlevel_client", [&isV2] { isV2 = false; });
    return isV2;
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem

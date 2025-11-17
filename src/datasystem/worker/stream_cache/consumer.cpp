/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include "datasystem/worker/stream_cache/consumer.h"

#include <chrono>
#include <utility>
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/consumer.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
Consumer::Consumer(std::string id, uint64_t lastAckCursor, std::string streamName, std::shared_ptr<Cursor> cursor)
    : id_(std::move(id)),
      streamName_(std::move(streamName)),
      initialCursor_(lastAckCursor),
      pendingRecv_(nullptr),
      cursor_(std::move(cursor))
{
    // Put the last ack cursor in the work area
    UpdateWALastAckCursor(initialCursor_);
}

Consumer::~Consumer() = default;

Status Consumer::AddPendingReceive(uint64_t lastRecvCursor, uint64_t timeoutMs, const std::function<void()> &recvFunc,
                                   std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> stream)
{
    PerfPoint point(PerfKey::WORKER_CONSUMER_ADD_PENDING_RECV);
    std::unique_lock<std::mutex> lock(mutex_);
    if (pendingRecv_) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_DUPLICATED,
                                FormatString("The consumer %s already had pending receive request.", id_));
    }
    INJECT_POINT("worker.stream.before_add_pending");
    TimerQueue::TimerImpl timer;
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(timeoutMs, recvFunc, timer));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Add pending receive request with timer:id %zu", LogPrefix(),
                                                timer.GetId());
    pendingRecv_ = std::make_unique<PendingReceive>();
    pendingRecv_->lastRecvCursor = lastRecvCursor;
    pendingRecv_->timer = std::make_unique<TimerQueue::TimerImpl>(timer);
    pendingRecv_->stream = std::move(stream);
    pendingRecv_->start = std::chrono::steady_clock::now();
    pendingRecv_->wakeupPendingRecvOnProdFault = false;
    return Status::OK();
}

void Consumer::RemovePendingReceive(bool &wakeupPendingRecvOnProdFault)
{
    std::lock_guard<std::mutex> lock(mutex_);
    RemovePendingReceiveNoLock(wakeupPendingRecvOnProdFault);
}

void Consumer::RemovePendingReceiveNoLock(bool &wakeupPendingRecvOnProdFault)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Remove pending receive request with timer:id %zu", LogPrefix(),
                                                pendingRecv_->timer->GetId());
    auto nowTime = std::chrono::steady_clock::now();
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTime - pendingRecv_->start).count();
    PerfPoint::RecordElapsed(PerfKey::PENDING_RECV_WAIT_TIME, elapsed);
    wakeupPendingRecvOnProdFault = pendingRecv_ != nullptr && pendingRecv_->wakeupPendingRecvOnProdFault;
    pendingRecv_ = nullptr;
}

Status Consumer::WakeUpPendingReceive(uint64_t lastAppendCursor)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // If this consumer has pendingRecv.
    // And its lastRecvCursor + expectRecvNum <= TargetStream.lastAppendCursor(Enough data to receive).
    if (pendingRecv_ != nullptr) {
        auto lastRecvCursor = pendingRecv_->lastRecvCursor;
        if (lastRecvCursor <= lastAppendCursor) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Do EraseAndExecTimer for timer:id %zu", LogPrefix(),
                                                        pendingRecv_->timer->GetId());
            (void)TimerQueue::GetInstance()->EraseAndExecTimer(*pendingRecv_->timer);
        }
    }
    return Status::OK();
}

std::string Consumer::LogPrefix() const
{
    return FormatString("C:%s", id_);
}

Status Consumer::SetForceClose()
{
    cursor_->SetForceClose();
    std::lock_guard<std::mutex> lock(mutex_);
    if (pendingRecv_ != nullptr) {
        pendingRecv_->wakeupPendingRecvOnProdFault = true;
        CHECK_FAIL_RETURN_STATUS(TimerQueue::GetInstance()->EraseAndExecTimer(*pendingRecv_->timer),
                                 StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Consumer %s failed to erase and exec timer on producer failure", id_));
    }
    return Status::OK();
}

void Consumer::CleanupConsumer()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (pendingRecv_ != nullptr) {
        (void)TimerQueue::GetInstance()->Cancel(*pendingRecv_->timer);
        pendingRecv_.reset();
    }
    initialCursor_ = 0;
    if (cursor_) {
        LOG_IF_ERROR(cursor_->Init(), FormatString("[%s] CleanupConsumer", LogPrefix()));
        cursor_->UpdateWALastAckCursor(0);
    }
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_CONSUMER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_CONSUMER_H

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class Consumer {
public:
    /**
     * @brief Construct Consumer.
     * @param[in] id The consumer id.
     * @param[in] cursorSet The cursor class.
     */
    Consumer(std::string id, uint64_t lastAckCursor, std::string streamName, std::shared_ptr<Cursor> cursor);
    virtual ~Consumer();

    struct PendingReceive {
        uint64_t lastRecvCursor;
        std::unique_ptr<TimerQueue::TimerImpl> timer;
        std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> stream;
        std::chrono::time_point<std::chrono::steady_clock> start;
        bool wakeupPendingRecvOnProdFault;
    };

    /**
     * @brief Add the pending receive to the consumer without locker protect, it should be call when the consumer can
     * not read enough elements and the timeout is greater than 0.
     * @param[in] lastRecvCursor The client last recv cursor.
     * @param[in] timer The timer add to the TimerQueue.
     * @param[in] stream The stream that used to write response to client.
     * @return Status of the call.
     */
    Status AddPendingReceive(uint64_t lastRecvCursor, uint64_t timeoutMs, const std::function<void()> &recvFunc,
                             std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> stream);

    /**
     * @brief Remove the pending receive, it should be call after the callback function is executed.
     */
    void RemovePendingReceive(bool &wakeupPendingRecvOnProdFault);

    /**
     * @brief Remove the pending receive without lock protect, it should be call after the callback function is
     * executed.
     */
    void RemovePendingReceiveNoLock(bool &wakeupPendingRecvOnProdFault);

    /**
     * @brief Wake up pending receive.
     * @param[in] lastAppendCursor The last append cursor of the stream.
     * @return Status of the call.
     */
    Status WakeUpPendingReceive(uint64_t lastAppendCursor);

    /**
     * @brief Get the consumer id.
     * @return The consumer id.
     */
    [[nodiscard]] std::string GetId() const
    {
        return id_;
    }

    /**
     * @brief Get log prefix.
     * @return Log prefix.
     */
    [[nodiscard]] virtual std::string LogPrefix() const;

    /**
     * @brief Get the last ack cursor from the work area
     * @return last ack cursor
     */
    [[nodiscard]] uint64_t GetWALastAckCursor() const
    {
        return cursor_->GetWALastAckCursor();
    }

    /**
     * @brief Update the last ack cursor from the work area
     * @param[in] elementId The element Id.
     * @return last ack cursor
     */
    void UpdateWALastAckCursor(uint64_t elementId) const
    {
        cursor_->UpdateWALastAckCursor(elementId);
    }

    /**
     * Force a consumer when there is no producer
     * @return
     */
    Status SetForceClose();

    /**
     * @brief Get the element count and reset it to 0.
     * @return The element count
     */
    uint64_t GetElementCountAndReset() const
    {
        return cursor_->GetElementCountAndReset();
    }

    /**
     * @brief Get the element count of the cursor
     * @return The element count
     */
    uint64_t GetElementCount() const
    {
        return cursor_->GetElementCount();
    }

    /**
     * @brief Get the request count of the cursor
     * @return The request count
     */
    uint64_t GetRequestCountAndReset() const
    {
        return cursor_->GetRequestCountAndReset();
    }

    /**
     * @brief Set the element count of the cursor
     * @param val value to set element count to
     */
    void SetElementCount(uint64_t val) const
    {
        return cursor_->SetElementCount(val);
    }

    /**
     * @brief Cleanup indexes and pending recv for this consumer.
     */
    void CleanupConsumer();

private:
    const std::string id_;
    const std::string streamName_;
    uint64_t initialCursor_;
    std::mutex mutex_;  // protect pendingRecv_
    std::unique_ptr<PendingReceive> pendingRecv_;
    // A work area that is shared between the corresponding client::stream_cache::ConsumerImpl
    // sz is the size of this work area. It is set up in the function ExclusivePageQueue::AddCursor
    std::shared_ptr<Cursor> cursor_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif

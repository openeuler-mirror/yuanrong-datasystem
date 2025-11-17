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

#include "datasystem/worker/stream_cache/subscription.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/stream_cache/consumer.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
Subscription::Subscription(SubscriptionConfig subConfig, uint64_t lastStreamAck, std::string streamName)
    : subConfig_(std::move(subConfig)), streamName_(std::move(streamName)), lastSubAckCursor_(lastStreamAck)
{
}

Status Subscription::AddConsumer(const SubscriptionConfig &config, const std::string &consumerId,
                                 uint64_t lastAckCursor, std::shared_ptr<Cursor> cursor)
{
    CHECK_FAIL_RETURN_STATUS(config == this->subConfig_, StatusCode::K_RUNTIME_ERROR,
                             "The subscription config is different.");
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    // Initialize cursor by SubscriptionType, available data range is [lastSubAckCursor_, lastAppendCursor).
    CHECK_FAIL_RETURN_STATUS(config.subscriptionType == SubscriptionType::STREAM, K_INVALID, "Not supported config.");
    CHECK_FAIL_RETURN_STATUS(consumers_.empty(), StatusCode::K_RUNTIME_ERROR,
                             "In STREAM mode, 1 Subscription can only contain 1 Consumer");
    // In stream mode, the consumer is the only one in current subscription, so we don't have to update ackCursor.
    auto consumer = std::make_shared<Consumer>(consumerId, lastAckCursor, streamName_, cursor);
    consumer->SetElementCount(lastAckCursor);
    auto ret = consumers_.emplace(consumerId, std::move(consumer));
    CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_DUPLICATED, "Failed to add consumer into subscription");
    return Status::OK();
}

Status Subscription::RemoveConsumer(const std::string &consumerId)
{
    std::shared_ptr<Consumer> consumerPtr;
    RETURN_IF_NOT_OK(GetConsumer(consumerId, consumerPtr));
    uint64_t consumerAck = consumerPtr->GetWALastAckCursor();
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    if (ConsumerNum() == 1) {  // If only one consumer left (stream mode or queue mode with the last consumer).
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Remove this sub since its last consumer is closed",
                                                    LogPrefix());
        consumers_.clear();
    } else {  // Otherwise
        CHECK_FAIL_RETURN_STATUS(
            consumers_.erase(consumerId) == 1, StatusCode::K_RUNTIME_ERROR,
            FormatString("Failed to remove consumer by consumerId %s in current Subscription", consumerId));
        if (consumerAck == lastSubAckCursor_) {
            // If target consumer's ack is the minimum in this subscription, we should update ack cursor.
            lastSubAckCursor_ = CalcMinAckCursorNoLock();
        }
    }
    return Status::OK();
}

SubscriptionType Subscription::GetSubscriptionType() const
{
    PerfPoint point(PerfKey::MANAGER_GET_SUB_TYPE);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return subConfig_.subscriptionType;
}

Status Subscription::GetConsumer(const std::string &consumerId, std::shared_ptr<Consumer> &consumer)
{
    PerfPoint point(PerfKey::MANAGER_GET_CONSUMER);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = consumers_.find(consumerId);
    if (iter == consumers_.end()) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Consumer not found " + consumerId);
    }
    RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
    consumer = iter->second;
    point.Record();
    return Status::OK();
}

std::string Subscription::LogPrefix() const
{
    return FormatString("Sub:%s", subConfig_.subscriptionName);
}

uint64_t Subscription::CalcMinAckCursorNoLock() const
{
    uint64_t newAckCursor = std::numeric_limits<uint64_t>::max();
    for (auto &ele : consumers_) {
        uint64_t lastAckCursor = ele.second->GetWALastAckCursor();
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s %s] lastAckCursor = %zu", LogPrefix(), ele.second->LogPrefix(),
                                                  lastAckCursor);
        newAckCursor = std::min<uint64_t>(newAckCursor, lastAckCursor);
    }
    return newAckCursor;
}

uint64_t Subscription::UpdateLastAckCursor()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto newAckCursor = CalcMinAckCursorNoLock();
    bool subAckForward = newAckCursor > lastSubAckCursor_;
    if (subAckForward) {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Update min ack cursor from %zu to %zu for subscription %s",
                                                  lastSubAckCursor_.load(), newAckCursor, subConfig_.subscriptionName);
        lastSubAckCursor_.store(newAckCursor);
    }
    return lastSubAckCursor_.load(std::memory_order_relaxed);
}

Status Subscription::TryWakeUpPendingReceive(uint64_t lastAppendCursor)
{
    if (subConfig_.subscriptionType != SubscriptionType::STREAM) {
        RETURN_STATUS(StatusCode::K_INVALID, "Only support stream mode");
    }
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &consumer : consumers_) {
        RETURN_IF_NOT_OK(consumer.second->WakeUpPendingReceive(lastAppendCursor));
    }
    return Status::OK();
}

Status Subscription::SetForceClose()
{
    if (subConfig_.subscriptionType != SubscriptionType::STREAM) {
        RETURN_STATUS(StatusCode::K_INVALID, "Only support stream mode");
    }
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    Status rc;
    for (const auto &consumer : consumers_) {
        Status rc1 = consumer.second->SetForceClose();
        if (rc.IsOk()) {
            rc = rc1;
        }
    }
    return rc;
}

uint64_t Subscription::GetElementCountAndReset()
{
    uint64_t val = 0;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &consumer : consumers_) {
        val += consumer.second->GetElementCountAndReset();
    }
    return val;
}

uint64_t Subscription::GetElementCountReceived()
{
    uint64_t val = std::numeric_limits<uint64_t>::max();
    uint64_t count = 0;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &consumer : consumers_) {
        count = consumer.second->GetElementCount();
        if (val > count) {
            val = count;
        }
    }
    return val;
}

uint64_t Subscription::GetRequestCountAndReset()
{
    uint64_t val = 0;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &consumer : consumers_) {
        val += consumer.second->GetRequestCountAndReset();
    }
    return val;
}

void Subscription::GetAllConsumers(std::vector<std::string> &consumers) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : consumers_) {
        auto &consumerName = kv.first;
        consumers.emplace_back(consumerName);
    }
}

void Subscription::CleanupSubscription()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : consumers_) {
        kv.second->CleanupConsumer();
    }
    lastSubAckCursor_ = 0;
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

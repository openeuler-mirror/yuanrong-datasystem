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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_SUBSCRIPTION_H
#define DATASYSTEM_WORKER_STREAM_CACHE_SUBSCRIPTION_H

#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class Consumer;
/**
 * @brief Two Modes, workload assignment logic is here.
 * @details For assignment see AssignCursors.
 */
class Subscription {
public:
    /**
     * @brief Create a new Subscription object.
     * @param[in] subConfig The config fo this subscription.
     * @param[in] lastStreamAck The stream ackCursor, which should be recorded by Subscription.
     */
    Subscription(SubscriptionConfig subConfig, uint64_t lastStreamAck, std::string streamName);
    ~Subscription() = default;

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * @param[in] config The subscription config.
     * @param[in] consumerId The id of consumer.
     * @param[out] lastAckCursor The last ack cursor of the consumer.
     * @return Status of the call.
     */
    Status AddConsumer(const SubscriptionConfig &config, const std::string &consumerId, uint64_t lastAckCursor,
                       std::shared_ptr<Cursor> cursor);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * @param[in] consumerId Consumer id.
     * @return Status of the call.
     */
    Status RemoveConsumer(const std::string &consumerId);

    /**
     * @brief Get consumer by id.
     * @param[in] consumerId The id of consumer.
     * @param[out] consumer The output consumer.
     * @return Status of the call.
     */
    Status GetConsumer(const std::string &consumerId, std::shared_ptr<Consumer> &consumer);

    /**
     * @brief Get subscription type.
     * @return SubscriptionType of the subscription.
     */
    SubscriptionType GetSubscriptionType() const;

    /**
     * @brief Get last ack cursor of the subscription.
     * @return The last ack cursor of the subscription.
     */
    uint64_t GetLastSubAckCursor() const
    {
        return lastSubAckCursor_;
    }

    /**
     * @brief Wake up pending receive if the element is enough.
     * @param[in] lastAppendCursor The last append cursor of the stream.
     * @return Status of the call.
     */
    Status TryWakeUpPendingReceive(uint64_t lastAppendCursor);

    /**
     * @brief Wake up pending receives if a producer is forcing consumers to be interrupted.
     * @return Status of the call.
     */
    Status SetForceClose();

    /**
     * @brief Get the num of consumer.
     * @return The num of consumer in one subscription.
     */
    size_t ConsumerNum() const
    {
        return consumers_.size();
    }

    /**
     * @brief Identity if or not this subscription has one consumer.
     * @return True if this subscription has one consumer.
     */
    bool HasConsumer() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return !consumers_.empty();
    }

    void GetAllConsumers(std::vector<std::string> &consumers) const;

    /**
     * @brief Get the subscription config.
     * @return The subscription config.
     */
    const SubscriptionConfig &GetSubscriptionConfig() const
    {
        return subConfig_;
    }

    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    std::string LogPrefix() const;

    /**
     * @brief Garbage collection by scanning all consumers' last ack cursors
     * @param[out] last ack cursor
     */
    uint64_t UpdateLastAckCursor();

    /**
     * @brief Get the element count and reset it to 0.
     * @return
     */
    uint64_t GetElementCountAndReset();
    
    /**
     * @brief Get the received element count.
     * @return
     */
    uint64_t GetElementCountReceived();

    /**
     * @brief Get the request count.
     * @return
     */
    uint64_t GetRequestCountAndReset();

    const std::string SubName() const
    {
        return subConfig_.subscriptionName;
    }

    /**
     * @brief Cleanup indexes for this subscription
     */
    void CleanupSubscription();

protected:
    /**
     * @brief Get the min ack cursor of all the consumers of this subscription.
     * @return The min ack cursor of all the consumers of this subscription.
     */
    uint64_t CalcMinAckCursorNoLock() const;

private:
    const SubscriptionConfig subConfig_;
    const std::string streamName_;
    std::atomic<uint64_t> lastSubAckCursor_;
    mutable std::shared_timed_mutex mutex_;  // protect consumers_
    std::unordered_map<std::string, std::shared_ptr<Consumer>> consumers_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_SUBSCRIPTION_H

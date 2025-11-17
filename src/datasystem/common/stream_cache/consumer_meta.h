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

/**
 * Description: The interface of file cache worker descriptor.
 */
#ifndef DATASYSTEM_COMMON_STREAM_CACHE_CONSUMER_META_H
#define DATASYSTEM_COMMON_STREAM_CACHE_CONSUMER_META_H

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
inline SubscriptionType ToSubscriptionType(SubscriptionTypePb typePb)
{
    switch (typePb) {
        case SubscriptionTypePb::STREAM_PB:
            return SubscriptionType::STREAM;
        case SubscriptionTypePb::KEY_PARTITIONS_PB:
            return SubscriptionType::KEY_PARTITIONS;
        case SubscriptionTypePb::ROUND_ROBIN_PB:
            return SubscriptionType::ROUND_ROBIN;
        default:
            return SubscriptionType::UNKNOWN;
    }
}

inline SubscriptionTypePb ToSubscriptionTypePb(SubscriptionType type)
{
    switch (type) {
        case SubscriptionType::STREAM:
            return SubscriptionTypePb::STREAM_PB;
        case SubscriptionType::KEY_PARTITIONS:
            return SubscriptionTypePb::KEY_PARTITIONS_PB;
        case SubscriptionType::ROUND_ROBIN:
            return SubscriptionTypePb::ROUND_ROBIN_PB;
        default:
            return SubscriptionTypePb::SubscriptionTypePb_INT_MIN_SENTINEL_DO_NOT_USE_;
    }
}

/**
 * @brief Consumer metadata.
 * @details Consisting of stream name, consumer id, worker address, subscription configuration and consumer last acked
 * cursor. This class is used in both worker and master components.
 */
class ConsumerMeta {
public:
    ConsumerMeta(std::string streamName, std::string consumerId, HostPort workerAddress,
                 SubscriptionConfig subConfig, uint64_t lastAckCursor)
        : streamName_(std::move(streamName)),
          consumerId_(std::move(consumerId)),
          workerAddress_(std::move(workerAddress)),
          subConfig_(std::move(subConfig)),
          lastAckCursor_(lastAckCursor)
    {
    }

    std::string ToString() const
    {
        std::string typeName = (subConfig_.subscriptionType == SubscriptionType::STREAM) ? "stream" : "queue";
        return FormatString("Stream: <%s>, Worker:<%s>, Subscription:<%s>, mode:<%s>, Consumer:<%s>", streamName_,
                            workerAddress_.ToString(), subConfig_.subscriptionName, typeName, consumerId_);
    }

    std::string StreamName() const
    {
        return streamName_;
    }

    HostPort WorkerAddress() const
    {
        return workerAddress_;
    }

    std::string ConsumerId() const
    {
        return consumerId_;
    }

    SubscriptionConfig SubConfig() const
    {
        return subConfig_;
    }

    uint64_t LastAckCursor() const
    {
        return lastAckCursor_;
    }

    inline bool operator<(const ConsumerMeta &other) const
    {
        return this->consumerId_ < other.consumerId_;
    }

    ConsumerMetaPb SerializeToPb() const
    {
        ConsumerMetaPb consumerMetaPb;
        consumerMetaPb.set_stream_name(streamName_);
        consumerMetaPb.mutable_worker_address()->set_host(workerAddress_.Host());
        consumerMetaPb.mutable_worker_address()->set_port(workerAddress_.Port());
        consumerMetaPb.set_consumer_id(consumerId_);
        consumerMetaPb.mutable_sub_config()->set_subscription_name(subConfig_.subscriptionName);
        consumerMetaPb.mutable_sub_config()->set_subscription_type(ToSubscriptionTypePb(subConfig_.subscriptionType));
        consumerMetaPb.set_last_ack_cursor(lastAckCursor_);
        return consumerMetaPb;
    }

    void ParseFromPb(const ConsumerMetaPb &metaPb)
    {
        streamName_ = metaPb.stream_name();
        workerAddress_ = HostPort(metaPb.worker_address().host(), metaPb.worker_address().port());
        consumerId_ = metaPb.consumer_id();
        subConfig_ = SubscriptionConfig(metaPb.sub_config().subscription_name(),
                                        ToSubscriptionType(metaPb.sub_config().subscription_type()));
        lastAckCursor_ = metaPb.last_ack_cursor();
    }

private:
    std::string streamName_;
    std::string consumerId_;
    HostPort workerAddress_;
    SubscriptionConfig subConfig_;
    uint64_t lastAckCursor_ = 0;  // Generated on worker side
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_STREAM_CACHE_CONSUMER_META_H

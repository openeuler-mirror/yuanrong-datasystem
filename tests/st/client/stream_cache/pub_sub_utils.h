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

#ifndef DATASYSTEM_PUB_SUB_UTILS_H
#define DATASYSTEM_PUB_SUB_UTILS_H

#include <unordered_map>
#include <vector>
#include <string>

#include "common/stream_cache/stream_common.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
namespace mock {
struct InputStreamInfo {
    int producerNum;
    std::unordered_map<std::string, std::pair<SubscriptionType, int>> subscriptions;
};

struct OutputStreamInfo {
    std::vector<std::shared_ptr<Producer>> producers;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Consumer>>> consumers;
};
}  // namespace mock

template <typename InputStreamInfoT, typename OutputStreamInfoT>
Status CreateProducersAndConsumers(std::shared_ptr<StreamClient> &client,
                                   std::unordered_map<std::string, InputStreamInfoT> &input,
                                   std::unordered_map<std::string, OutputStreamInfoT> &output)
{
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    output.reserve(input.size());
    for (auto &iter : input) {
        std::string streamName = iter.first;
        auto &info = iter.second;
        // Create producers for the stream
        output[streamName].producers.reserve(info.producerNum);
        for (int i = 0; i < info.producerNum; i++) {
            LOG(INFO) << "Start create producer " << i << " for stream " << streamName;
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, conf));
            LOG(INFO) << "Finished create producer for stream " << streamName;
            output[streamName].producers.push_back(std::move(producer));
        }
        // Create consumers for the subscriptions
        for (auto &subInfo : info.subscriptions) {
            auto &subName = subInfo.first;
            SubscriptionConfig config(subName, subInfo.second.first);
            int consumerNum = subInfo.second.second;
            output[streamName].consumers[subName].reserve(consumerNum);
            for (int i = 0; i < consumerNum; i++) {
                LOG(INFO) << "Start create consumer" << i << " for stream " << streamName;
                std::shared_ptr<Consumer> consumer;
                RETURN_IF_NOT_OK(client->Subscribe(streamName, config, consumer));
                LOG(INFO) << "Finished create consumer for sub" << subName;
                output[streamName].consumers[subName].push_back(std::move(consumer));
            }
        }
    }
    return Status::OK();
}
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_PUB_SUB_UTILS_H

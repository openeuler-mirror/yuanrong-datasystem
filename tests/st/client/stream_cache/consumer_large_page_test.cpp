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
 * Description: Unit test for stream cache
 */
#include <vector>
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "common/stream_cache/element_generator.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "client/stream_cache/pub_sub_utils.h"
using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class ConsumerLargePageTest : public SCClientCommon {
public:
    explicit ConsumerLargePageTest(int pageSize = 1024 * 1024) : pageSize_(pageSize)
    {
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.workerGflagParams = " -page_size=" + std::to_string(pageSize_);
        opts.numRpcThreads = 0;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        client_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    using InputStreamInfo = mock::InputStreamInfo;
    using OutputStreamInfo = mock::OutputStreamInfo;

    Status CreateProducersAndConsumers(std::unordered_map<std::string, InputStreamInfo> &input,
                                       std::unordered_map<std::string, OutputStreamInfo> &output)
    {
        return datasystem::st::CreateProducersAndConsumers(client_, input, output);
    }

    std::vector<Element> GenerateElements(int elementNum, uint64_t elementSize, std::string &outData)
    {
        outData = RandomData().GetRandomString(elementNum * elementSize);
        std::vector<Element> ret;
        ret.reserve(elementSize);
        for (int i = 1; i <= elementNum; i++) {
            Element element((uint8_t *)(outData.data()), elementSize, ULONG_MAX);
            ret.push_back(element);
        }
        return ret;
    }

protected:
    void InitTest()
    {
        InitStreamClient(0, client_);
    }
    std::shared_ptr<StreamClient> client_ = nullptr;
    uint64_t pageSize_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(ConsumerLargePageTest, SendRecvManyElements)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "testSendRecvManyEle";

    const uint64_t maxStreamSize = 64 * 1024 * 1024;  // 64M;
    const uint64_t pageSize = 1024 * 1024;            // 1M;
    DS_ASSERT_OK(client_->CreateProducer(
        streamName, producer, { .delayFlushTime = -1, .pageSize = pageSize, .maxStreamSize = maxStreamSize }));
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));
    ElementGenerator generator(1024, 1024);
    auto strs = generator.GenElements("producer", 4000);
    for (int round = 0; round < 100; round++) {
        for (int i = 0; i < 4000; i++) {
            ASSERT_EQ(producer->Send(Element{ (uint8_t *)(strs[i].c_str()), strs[i].size() }), Status::OK());
        }
        std::vector<Element> elements;
        consumer->Receive(4000, 0, elements);
        for (auto &e : elements) {
            ASSERT_EQ(Status::OK(), ElementView(std::string((char *)e.ptr, e.size)).VerifyIntegrity());
        }
        consumer->Ack(elements.back().id);
    }
}

TEST_F(ConsumerLargePageTest, PageSizeExceedsStreamSize)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "testPgSzExceedsStreamSz";

    const uint64_t maxStreamSize = 999 * 1024 * 1024;  // 999M;
    const uint64_t pageSize = 1024 * 1024 * 1024;      // 1GB;
    ASSERT_EQ(datasystem::StatusCode::K_INVALID, (client_->CreateProducer(
        streamName, producer, { .delayFlushTime = -1, .pageSize = pageSize,
        .maxStreamSize = maxStreamSize })).GetCode());
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));
}
}  // namespace st
}  // namespace datasystem

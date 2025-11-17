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
#include <gtest/gtest.h>
#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/stream/stream_config.h"
#include "sc_client_common.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class PubSubTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
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

protected:
    void InitTest()
    {
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        ConnectOptions options;
        options.accessKey = accessKey_;
        options.secretKey = secretKey_;
        options.host = workerAddress.Host();
        options.port = workerAddress.Port();
        client_ = std::make_shared<StreamClient>(options);
        EXPECT_NE(client_, nullptr);
        DS_ASSERT_OK(client_->Init());
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }
    std::shared_ptr<StreamClient> client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

class SingleStreamTest : public PubSubTest {};

TEST_F(SingleStreamTest, MultiSubMultiConsumer)
{
    std::string stream1("singleStream");
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumer));
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config2, consumer2));

    // The last two should fail
    std::shared_ptr<Consumer> consumer3;
    DS_EXPECT_NOT_OK(client_->Subscribe(stream1, config, consumer3));
    std::shared_ptr<Consumer> consumer4;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config2, consumer4));
}

class MultiStreamTest : public PubSubTest {};
// Single sub test for multi stream
TEST_F(MultiStreamTest, SingleProducerSingleConsumerWithOneSub)
{
    // Create stream1 with one producer and one consumer in one subscription
    std::string stream1("stream1_SPSCSS");
    std::shared_ptr<Producer> producerS1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producerS1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerS1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumerS1));

    // Create stream2 with one producer and one consumer in one subscription
    std::string stream2("stream2_SPSCSS");
    std::shared_ptr<Producer> producerS2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producerS2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerS2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumerS2));
}

TEST_F(MultiStreamTest, SingleProducerMultiConsumerWithOneSub)
{
    // Create stream1 with one producer and two consumers in one subscription
    std::string stream1("stream1_SPMCSS");
    std::shared_ptr<Producer> producerS1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producerS1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumer1S1));
    std::shared_ptr<Consumer> consumer2S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config, consumer2S1));

    // Create stream2 with one producer and two consumers in one subscription
    std::string stream2("stream2_SPMCSS");
    std::shared_ptr<Producer> producerS2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producerS2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumer1S2));
    std::shared_ptr<Consumer> consumer2S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config, consumer2S2));
}

TEST_F(MultiStreamTest, MultiProducerSingleConsumerWithOneSub)
{
    // Create stream1 with two producer and one consumer
    std::string stream1("stream1_MPSCSS");
    std::shared_ptr<Producer> producer1S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer1S1, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer2S1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerS1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumerS1));

    // Create stream2 with one producer and one consumer
    std::string stream2("stream2_MPSCSS");
    std::shared_ptr<Producer> producer1S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer1S2, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer2S2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerS2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumerS2));
}

TEST_F(MultiStreamTest, MultiProducerMultiConsumerWithOneSub)
{
    // Create stream1 with two producer and two consumer in one subscription
    std::string stream1("stream1_MPMCSS");
    std::shared_ptr<Producer> producer1S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer1S1, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer2S1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumer1S1));
    std::shared_ptr<Consumer> consumer2S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config, consumer2S1));

    // Create stream2 with two producer and two consumer in one subscription
    std::string stream2("stream2_MPMCSS");
    std::shared_ptr<Producer> producer1S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer1S2, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer2S2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumer1S2));
    std::shared_ptr<Consumer> consumer2S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config, consumer2S2));
}

// Multi sub test for multi stream
TEST_F(MultiStreamTest, SingleProducerSingleConsumerWithMultiSub)
{
    // Create stream1 with one producer and two subscription(each subscription have one consumer)
    std::string stream1("stream1_SPSCMS");
    std::shared_ptr<Producer> producerS1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producerS1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerSub1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumerSub1S1));
    std::shared_ptr<Consumer> consumerSub2S1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config2, consumerSub2S1));

    // Create stream2 with one producer and one consumer in one subscription
    std::string stream2("stream2_SPSCMS");
    std::shared_ptr<Producer> producerS2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producerS2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerSub1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumerSub1S2));
    std::shared_ptr<Consumer> consumerSub2S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config2, consumerSub2S2));
}

TEST_F(MultiStreamTest, SingleProducerMultiConsumerWithMultiSub)
{
    // Create stream1 with one producer and two subscription, each subscription have two consumer
    std::string stream1("stream1_SPMCMS");
    std::shared_ptr<Producer> producerS1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producerS1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1Sub1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumer1Sub1S1));
    std::shared_ptr<Consumer> consumer2Sub1S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config, consumer2Sub1S1));
    std::shared_ptr<Consumer> consumer1Sub2S1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config2, consumer1Sub2S1));
    std::shared_ptr<Consumer> consumer2Sub2S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config2, consumer2Sub2S1));

    // Create stream2 with one producer and two consumer in one subscription
    std::string stream2("stream2_SPMCMS");
    std::shared_ptr<Producer> producerS2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producerS2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1Sub1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumer1Sub1S2));
    std::shared_ptr<Consumer> consumer2Sub1S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config, consumer2Sub1S2));
    std::shared_ptr<Consumer> consumer1Sub2S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config2, consumer1Sub2S2));
    std::shared_ptr<Consumer> consumer2Sub2S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config2, consumer2Sub2S2));
}

TEST_F(MultiStreamTest, MultiProducerSingleConsumerWithMultiSub)
{
    // Create stream1 with two producer and two subscription each subscription have one consumer
    std::string stream1("stream1_MPSCMS");
    std::shared_ptr<Producer> producer1S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer1S1, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer2S1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerSub1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumerSub1S1));
    std::shared_ptr<Consumer> consumerSub2S1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config2, consumerSub2S1));

    // Create stream2 with two producer and two subscription each subscription have one consumer
    std::string stream2("stream2_MPSCMS");
    std::shared_ptr<Producer> producer1S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer1S2, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer2S2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumerSub1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumerSub1S2));
    std::shared_ptr<Consumer> consumerSub2S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config2, consumerSub2S2));
}

TEST_F(MultiStreamTest, MultiProducerMultiConsumerWithMultiSub)
{
    // Create stream1 with two producer and two subscription , each subscription have two consumer
    std::string stream1("stream1_MPMCMS");
    std::shared_ptr<Producer> producer1S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer1S1, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S1;
    DS_ASSERT_OK(client_->CreateProducer(stream1, producer2S1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1Sub1S1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config, consumer1Sub1S1));
    std::shared_ptr<Consumer> consumer2Sub1S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config, consumer2Sub1S1));
    std::shared_ptr<Consumer> consumer1Sub2S1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream1, config2, consumer1Sub2S1));
    std::shared_ptr<Consumer> consumer2Sub2S1;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream1, config2, consumer2Sub2S1));

    // Create stream2 with two producer and two subscription, each subscription have two consumer
    std::string stream2("stream2_MPMCMS");
    std::shared_ptr<Producer> producer1S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer1S2, defaultProducerConf_));
    std::shared_ptr<Producer> producer2S2;
    DS_ASSERT_OK(client_->CreateProducer(stream2, producer2S2, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1Sub1S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config, consumer1Sub1S2));
    std::shared_ptr<Consumer> consumer2Sub1S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config, consumer2Sub1S2));
    std::shared_ptr<Consumer> consumer1Sub2S2;
    DS_ASSERT_OK(client_->Subscribe(stream2, config2, consumer1Sub2S2));
    std::shared_ptr<Consumer> consumer2Sub2S2;
    DS_ASSERT_NOT_OK(client_->Subscribe(stream2, config2, consumer2Sub2S2));
}
}  // namespace st
}  // namespace datasystem

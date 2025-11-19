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
 * Description: Test ObjectMeta Storage basic functions.
 */
#include <memory>

#include <gtest/gtest.h>

#include "common.h"

#include "common/stream_cache/stream_common.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/worker/stream_cache/worker_master_sc_api.h"

namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
class PubSubTopoTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numRpcThreads = 1;
        opts.numEtcd = 1;
        opts.isStreamCacheCase = true;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        client1_ = nullptr;
        client2_ = nullptr;
        client3_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    void InitStreamClient(uint32_t index, std::shared_ptr<StreamClient> &client, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(index < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions connectOptions;
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        client = std::make_shared<StreamClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

protected:
    void InitTest()
    {
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress1_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress2_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(2, workerAddress3_));
        LOG(INFO) << FormatString("\n Worker1: <%s>\n Worker2: <%s>\n Worker3: <%s>", workerAddress1_.ToString(),
                                  workerAddress2_.ToString(), workerAddress3_.ToString());
        InitStreamClient(0, client1_);
        InitStreamClient(1, client2_);
        InitStreamClient(2, client3_); // index is 2.
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    HostPort workerAddress1_;
    HostPort workerAddress2_;
    HostPort workerAddress3_;

    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(PubSubTopoTest, SingleStreamSingleProducerSingleConsumerBySequence)
{
    std::string stream1("stream1");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    uint64_t producerNum = 0;
    uint64_t consumerNum = 0;

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    DS_ASSERT_OK(client1_->QueryGlobalProducersNum(stream1, producerNum));

    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));

    producerNum = 0;
    consumerNum = 0;
    std::shared_ptr<Producer> node3Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node3Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));
    DS_ASSERT_OK(client3_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client3_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(3));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(client2_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client2_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(3));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(client1_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client1_->QueryGlobalConsumersNum(stream1, consumerNum));

    EXPECT_EQ(consumerNum, size_t(3));

    consumerNum = 0;
    DS_ASSERT_OK(node1Consumer1->Close());
    DS_ASSERT_OK(client2_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(2));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(client3_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client3_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(2));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(node2Consumer1->Close());
    DS_ASSERT_OK(client1_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client2_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(1));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(node3Producer1->Close());
    DS_ASSERT_OK(client2_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client3_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(1));

    producerNum = 0;
    consumerNum = 0;
    DS_ASSERT_OK(node3Consumer1->Close());
    DS_ASSERT_OK(client1_->QueryGlobalProducersNum(stream1, producerNum));
    DS_ASSERT_OK(client3_->QueryGlobalConsumersNum(stream1, consumerNum));
    EXPECT_EQ(consumerNum, size_t(0));
}

TEST_F(PubSubTopoTest, SingleStreamSingleProducerSingleConsumerByRandom)
{
    std::string stream1("stream1");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));
    std::shared_ptr<Consumer> node3Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));

    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(node1Consumer1->Close());
    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(node2Consumer1->Close());
    DS_ASSERT_OK(node3Producer1->Close());
    DS_ASSERT_OK(node3Consumer1->Close());
}

TEST_F(PubSubTopoTest, MultiStreamSingleProducerSingleConsumerBySequence)
{
    std::string stream1("stream1");
    std::string stream2("stream2");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Stream1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Stream1Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node2Stream1Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Stream1Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Stream1Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Stream1Producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> node1Stream1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Stream1Consumer1));
    std::shared_ptr<Consumer> node2Stream1Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Stream1Consumer1));
    std::shared_ptr<Consumer> node3Stream1Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Stream1Consumer1));

    DS_ASSERT_OK(node1Stream1Producer1->Close());
    DS_ASSERT_OK(node1Stream1Consumer1->Close());
    DS_ASSERT_OK(node2Stream1Producer1->Close());
    DS_ASSERT_OK(node2Stream1Consumer1->Close());
    DS_ASSERT_OK(node3Stream1Producer1->Close());
    DS_ASSERT_OK(node3Stream1Consumer1->Close());

    std::shared_ptr<Producer> node1Stream2Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Stream2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node2Stream2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Stream2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Stream2Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Stream2Producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> node1Stream2Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Stream2Consumer1));
    std::shared_ptr<Consumer> node2Stream2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Stream2Consumer1));
    std::shared_ptr<Consumer> node3Stream2Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Stream2Consumer1));

    DS_ASSERT_OK(node1Stream2Producer1->Close());
    DS_ASSERT_OK(node1Stream2Consumer1->Close());
    DS_ASSERT_OK(node2Stream2Producer1->Close());
    DS_ASSERT_OK(node2Stream2Consumer1->Close());
    DS_ASSERT_OK(node3Stream2Producer1->Close());
    DS_ASSERT_OK(node3Stream2Consumer1->Close());
}

TEST_F(PubSubTopoTest, MultiStreamSingleProducerSingleConsumerByRandom)
{
    std::string stream1("stream1");
    std::string stream2("stream2");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    // n1p1->n3c1->n2p1->n1c1->n3p1->n2c1
    std::shared_ptr<Producer> node1Stream1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Stream1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node3Stream1Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Stream1Consumer1));
    std::shared_ptr<Producer> node2Stream1Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Stream1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Stream1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Stream1Consumer1));
    std::shared_ptr<Producer> node3Stream1Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Stream1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Stream1Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Stream1Consumer1));

    DS_ASSERT_OK(node3Stream1Consumer1->Close());
    DS_ASSERT_OK(node1Stream1Producer1->Close());
    DS_ASSERT_OK(node3Stream1Producer1->Close());
    DS_ASSERT_OK(node1Stream1Consumer1->Close());
    DS_ASSERT_OK(node2Stream1Consumer1->Close());
    DS_ASSERT_OK(node2Stream1Producer1->Close());

    // n2c1->n3c1->n1p1->n3p1->n1c1->n2p1
    std::shared_ptr<Consumer> node2Stream2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Stream2Consumer1));
    std::shared_ptr<Consumer> node3Stream2Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Stream2Consumer1));
    std::shared_ptr<Producer> node1Stream2Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Stream2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Stream2Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Stream2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Stream2Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Stream2Consumer1));
    std::shared_ptr<Producer> node2Stream2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Stream2Producer1, defaultProducerConf_));

    // n3c1->n2p1->n1c1->n1p1->n2c1->n3p1
    DS_ASSERT_OK(node3Stream2Consumer1->Close());
    DS_ASSERT_OK(node2Stream2Producer1->Close());
    DS_ASSERT_OK(node1Stream2Consumer1->Close());
    DS_ASSERT_OK(node1Stream2Producer1->Close());
    DS_ASSERT_OK(node2Stream2Consumer1->Close());
    DS_ASSERT_OK(node3Stream2Producer1->Close());
}

TEST_F(PubSubTopoTest, SingleStreamMultiProducerSingleConsumerBySequence)
{
    std::string stream1("stream1");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node1Producer2;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node2Producer2;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));

    std::shared_ptr<Producer> node3Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Producer2;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node3Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node3Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));

    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(node1Producer2->Close());
    DS_ASSERT_OK(node1Consumer1->Close());

    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(node2Producer2->Close());
    DS_ASSERT_OK(node2Consumer1->Close());

    DS_ASSERT_OK(node3Producer1->Close());
    DS_ASSERT_OK(node3Producer2->Close());
    DS_ASSERT_OK(node3Consumer1->Close());
}

TEST_F(PubSubTopoTest, SingleStreamMultiProducerMultiConsumerBySequence)
{
    std::string stream1("stream1");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config1Cpy("sub1_cpy", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config2Cpy("sub2_cpy", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    SubscriptionConfig config3Cpy("sub3_cpy", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node1Producer2;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));
    std::shared_ptr<Consumer> node1Consumer2;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1Cpy, node1Consumer2));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node2Producer2;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));
    std::shared_ptr<Consumer> node2Consumer2;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2Cpy, node2Consumer2));

    std::shared_ptr<Producer> node3Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));
    std::shared_ptr<Producer> node3Producer2;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node3Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node3Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));
    std::shared_ptr<Consumer> node3Consumer2;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3Cpy, node3Consumer2));

    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(node1Producer2->Close());
    DS_ASSERT_OK(node1Consumer1->Close());
    DS_ASSERT_OK(node1Consumer2->Close());

    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(node2Producer2->Close());
    DS_ASSERT_OK(node2Consumer1->Close());
    DS_ASSERT_OK(node2Consumer2->Close());

    DS_ASSERT_OK(node3Producer1->Close());
    DS_ASSERT_OK(node3Producer2->Close());
    DS_ASSERT_OK(node3Consumer1->Close());
    DS_ASSERT_OK(node3Consumer2->Close());
}
}  // namespace st
}  // namespace datasystem

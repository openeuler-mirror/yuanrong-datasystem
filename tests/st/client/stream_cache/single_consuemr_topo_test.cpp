/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test single consumer topo test.
 */
#include <mutex>
#include <string>
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/utils/status.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
constexpr int K_TWO = 2;
class SingleConsumerTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -log_monitor=true ";
        opts.numRpcThreads = 0;
        opts.vLogLevel = DEFAULT_LOG_LEVEL;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(K_TWO, w3Addr_));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, K_TWO));
        // Worker 1.
        InitStreamClient(0, w1Client_);
        InitStreamClient(1, w2Client_);
        InitStreamClient(2, w3Client_); // index is 2
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    void TearDown() override
    {
        w1Client_ = nullptr;
        w2Client_ = nullptr;
        w3Client_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;
    HostPort w3Addr_;

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    std::shared_ptr<StreamClient> w3Client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const int DEFAULT_WAIT_TIME = 5000;
    const int DEFAULT_WORKER_NUM = 3;
    const int DEFAULT_LOG_LEVEL = 2;
};

class MPSCTest : public SingleConsumerTest {};

// create producer -> create consumer -> create consumer
TEST_F(MPSCTest, TestCreateMultiConsumer1)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    {
        // same worker.
        std::string stream1("singleStream1");
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        std::shared_ptr<Consumer> consumer2;
        DS_ASSERT_OK(w1Client_->CreateProducer(stream1, producer, producerConf));
        DS_ASSERT_OK(w1Client_->Subscribe(stream1, config, consumer));
        DS_ASSERT_NOT_OK(w1Client_->Subscribe(stream1, config2, consumer2));
    }

    {
        // diff worker.
        std::string stream2("singleStream2");
        std::shared_ptr<Producer> producerW1;
        std::shared_ptr<Consumer> consumerW2;
        std::shared_ptr<Consumer> consumerW3;
        DS_ASSERT_OK(w1Client_->CreateProducer(stream2, producerW1, producerConf));
        DS_ASSERT_OK(w2Client_->Subscribe(stream2, config, consumerW2));
        DS_ASSERT_NOT_OK(w3Client_->Subscribe(stream2, config2, consumerW3));
    }
}

// create consumer -> create producer -> create consumer
TEST_F(MPSCTest, TestCreateMultiConsumer2)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    {
        std::string stream1("singleStream1");
        std::shared_ptr<Consumer> consumer;
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer2;
        DS_ASSERT_OK(w1Client_->Subscribe(stream1, config, consumer));
        DS_ASSERT_OK(w1Client_->CreateProducer(stream1, producer, producerConf));
        DS_ASSERT_NOT_OK(w1Client_->Subscribe(stream1, config2, consumer2));
    }

    {
        std::string stream2("singleStream2");
        std::shared_ptr<Consumer> consumerW1;
        std::shared_ptr<Producer> producerW2;
        std::shared_ptr<Consumer> consumerW3;
        DS_ASSERT_OK(w1Client_->Subscribe(stream2, config, consumerW1));
        DS_ASSERT_OK(w2Client_->CreateProducer(stream2, producerW2, producerConf));
        DS_ASSERT_NOT_OK(w3Client_->Subscribe(stream2, config2, consumerW3));
    }
}

// create consumer -> create consumer -> create producer
TEST_F(MPSCTest, TestCreateMultiConsumer3)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    {
        std::string stream1("singleStream1");
        std::shared_ptr<Consumer> consumer;
        std::shared_ptr<Consumer> consumer2;
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w1Client_->Subscribe(stream1, config, consumer));
        DS_ASSERT_OK(w1Client_->Subscribe(stream1, config2, consumer2));
        DS_ASSERT_NOT_OK(w1Client_->CreateProducer(stream1, producer, producerConf));
    }

    {
        std::string stream2("singleStream2");
        std::shared_ptr<Consumer> consumerW1;
        std::shared_ptr<Consumer> consumerW2;
        std::shared_ptr<Producer> producerW3;
        DS_ASSERT_OK(w1Client_->Subscribe(stream2, config, consumerW1));
        DS_ASSERT_OK(w2Client_->Subscribe(stream2, config2, consumerW2));
        DS_ASSERT_NOT_OK(w3Client_->CreateProducer(stream2, producerW3, producerConf));
    }
}

TEST_F(MPSCTest, TestCreateMultiProducer)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    {
        std::string stream1("singleStream1");
        std::shared_ptr<Consumer> consumer;
        std::shared_ptr<Producer> producer1;
        std::shared_ptr<Producer> producer2;
        DS_ASSERT_OK(w1Client_->Subscribe(stream1, config, consumer));
        DS_ASSERT_OK(w1Client_->CreateProducer(stream1, producer1, producerConf));
        DS_ASSERT_OK(w1Client_->CreateProducer(stream1, producer2, producerConf));
    }

    {
        std::string stream2("singleStream2");
        std::shared_ptr<Consumer> consumerW1;
        std::shared_ptr<Producer> producer1W1;
        std::shared_ptr<Producer> producer2W2;
        std::shared_ptr<Producer> producer3W3;
        DS_ASSERT_OK(w1Client_->Subscribe(stream2, config, consumerW1));
        DS_ASSERT_OK(w1Client_->CreateProducer(stream2, producer1W1, producerConf));
        DS_ASSERT_OK(w2Client_->CreateProducer(stream2, producer2W2, producerConf));
        DS_ASSERT_OK(w3Client_->CreateProducer(stream2, producer3W3, producerConf));
    }
}

class SPSCTest : public SingleConsumerTest {};

// test create multi producer in SPSC mode.
TEST_F(SPSCTest, TestCreateMultiProducer)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::SPSC;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);

    std::string stream1("singleStream1");
    std::shared_ptr<Consumer> consumer;
    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->Subscribe(stream1, config, consumer));
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, producer1, producerConf));
    DS_ASSERT_NOT_OK(w1Client_->CreateProducer(stream1, producer2, producerConf));
    DS_ASSERT_NOT_OK(w2Client_->CreateProducer(stream1, producer2, producerConf));
}

class ShmReserveTest : public SingleConsumerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        SingleConsumerTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams = " -shared_memory_size_mb=32 ";
    }

protected:
    const int maxStreamSize_ = 16 * MB;
};

TEST_F(ShmReserveTest, TestMPMCProducerCreateOOM)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPMC;
    producerConf.maxStreamSize = maxStreamSize_;
    const int streamCount = 33;
    std::vector<std::shared_ptr<Producer>> producers;
    Status rc;
    int i = 0;
    for (; i < streamCount; i++) {
        auto streamName = "stream-" + std::to_string(i);
        std::shared_ptr<Producer> producer;
        rc = w1Client_->CreateProducer(streamName, producer, producerConf);
        if (rc.IsError()) {
            break;
        }
        producers.emplace_back(std::move(producer));
    }
    LOG(INFO) << "stream count:" << i;
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
}

TEST_F(ShmReserveTest, TestMPSCDiffNodeProducerNotOOM)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = maxStreamSize_;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    const int streamCount = 33;
    const int producerCountPerStream = 3;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    for (int i = 0; i < streamCount; i++) {
        auto streamName = "stream-" + std::to_string(i);
        std::shared_ptr<Consumer> consumer;
        const int subsWorkerCount = 2;
        if (i % subsWorkerCount == 0) {
            DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
        } else {
            DS_ASSERT_OK(w3Client_->Subscribe(streamName, config, consumer));
        }
        consumers.emplace_back(std::move(consumer));

        for (int j = 0; j < producerCountPerStream; j++) {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, producerConf));
            producers.emplace_back(std::move(producer));
        }
    }
}

TEST_F(ShmReserveTest, TestMPSCSomeNodeSubscribeOOM)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = maxStreamSize_;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    const int streamCount = 33;
    const int producerCountPerStream = 3;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    Status rc;
    int i = 0;
    for (; i < streamCount; i++) {
        auto streamName = "stream-" + std::to_string(i);
        for (int j = 0; j < producerCountPerStream; j++) {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, producerConf));
            producers.emplace_back(std::move(producer));
        }
        std::shared_ptr<Consumer> consumer;
        rc = (w1Client_->Subscribe(streamName, config, consumer));
        if (rc.IsError()) {
            break;
        }
        consumers.emplace_back(std::move(consumer));
    }
    LOG(INFO) << "stream count:" << i;
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
}

TEST_F(ShmReserveTest, TestMPSCSomeNodeProducerOOM)
{
    ProducerConf producerConf;
    producerConf.streamMode = StreamMode::MPSC;
    producerConf.maxStreamSize = maxStreamSize_;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    const int streamCount = 33;
    const int producerCountPerStream = 3;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    Status rc;
    int i = 0;
    for (; i < streamCount; i++) {
        auto streamName = "stream-" + std::to_string(i);
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
        if (rc.IsError()) {
            break;
        }
        consumers.emplace_back(std::move(consumer));
        for (int j = 0; j < producerCountPerStream; j++) {
            std::shared_ptr<Producer> producer;
            rc = (w1Client_->CreateProducer(streamName, producer, producerConf));
            if (rc.IsOk()) {
                break;
            }
            producers.emplace_back(std::move(producer));
        }
    }
    LOG(INFO) << "stream count:" << i;
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
}

}  // namespace st
}  // namespace datasystem

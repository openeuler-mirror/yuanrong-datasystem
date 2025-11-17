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
#include "sc_client_common.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
constexpr uint64_t MB = 1024 * 1024;
class StreamSizeTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int vLogLevel = 3;
        const int numWorkers = 3;
        opts.masterIdx = 2;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.vLogLevel = vLogLevel;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        int index = 0;
        DS_ASSERT_OK(InitClient(index++, client1_));
        DS_ASSERT_OK(InitClient(index++, client2_));
    }

    void TearDown() override
    {
        client1_ = nullptr;
        client2_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions options;
        options.host = workerAddress.Host();
        options.port = workerAddress.Port();
        options.secretKey = secretKey_;
        options.accessKey = accessKey_;
        client = std::make_shared<StreamClient>(options);
        return client->Init();
    }

    Status CreateConsumer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          const std::string &subName, std::shared_ptr<Consumer> &consumer)
    {
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        return client->Subscribe(streamName, config, consumer);
    }

    Status CreateProducer(std::shared_ptr<StreamClient> client, const std::string &streamName, uint64_t maxStreamSize,
                          std::shared_ptr<Producer> &producer)
    {
        ProducerConf conf;
        conf.maxStreamSize = maxStreamSize;
        return client->CreateProducer(streamName, producer, conf);
    }

    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamSizeTest, TestCreateProducerWithDiffSize)
{
    std::string streamName = "CreateProducerWithDiffSize";
    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Producer> producer3;
    std::shared_ptr<Producer> producer4;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer1));
    DS_ASSERT_NOT_OK(CreateProducer(client1_, streamName, 12 * MB, producer2));
    DS_ASSERT_NOT_OK(CreateProducer(client2_, streamName, 12 * MB, producer3));
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer4));
}

TEST_F(StreamSizeTest, TestCreateProducerThenConsumer)
{
    std::string streamName = "CreateProducerThenConsumer";
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub", consumer));

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());
}

TEST_F(StreamSizeTest, TestCreateConsumerThenProducer)
{
    std::string streamName = "CreateConsumerThenProducer";
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub", consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer));

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());
}

TEST_F(StreamSizeTest, TestCreateProducerThenConsumerTwoWorker)
{
    std::string streamName = "CreateProducerThenConsumerTwoWorker";
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer));
    LOG(INFO) << "Created producer";
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "sub", consumer));
    LOG(INFO) << "Created consumer";

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    LOG(INFO) << "do send";
    DS_ASSERT_OK(producer->Send(element));
    LOG(INFO) << "do flush";
    std::vector<Element> outElements;
    const uint32_t waitTime = 3000;  // 3s;
    LOG(INFO) << "do consumer receive";
    DS_ASSERT_OK(consumer->Receive(1, waitTime, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    LOG(INFO) << "do consumer and producer close";
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());
}

TEST_F(StreamSizeTest, TestCreateConsumerThenProducerTwoWorker)
{
    std::string streamName = "CreateConsumerThenProducerTwoWorker";
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub", consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client2_, streamName, 10 * MB, producer));

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    const uint32_t waitTime = 3000;  // 3s;
    DS_ASSERT_OK(consumer->Receive(1, waitTime, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());
}

TEST_F(StreamSizeTest, LEVEL1_TestCreateProducerAfterMasterRestart)
{
    std::shared_ptr<Producer> producer1;
    std::string streamName = "CreateProducerAfterMasterRestart";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 10 * MB, producer1));

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_NOT_OK(CreateProducer(client2_, streamName, 12 * MB, producer2));
}

TEST_F(StreamSizeTest, TestCreateProducerWorkerFailed)
{
    std::string streamName = "CreateProducerWorkerFailed";
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CreateProducer.beforeSendToMaster",
                                           "1*return(K_RUNTIME_ERROR)"));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_NOT_OK(CreateProducer(client1_, streamName, 10 * MB, producer1));

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, 12 * MB, producer2));
}

class StreamSizeCentralizedTest : public StreamSizeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableDistributedMaster = "false";
        StreamSizeTest::SetClusterSetupOptions(opts);
    }
};

TEST_F(StreamSizeCentralizedTest, TestCreateProducerMasterFailed)
{
    // Master has to be centralized for this testcase to work.
    DS_ASSERT_OK(cluster_->SetInjectAction(
        ClusterNodeType::WORKER, 2, "master.PubIncreaseNodeImpl.beforeSendNotification", "1*return(K_RUNTIME_ERROR)"));

    std::shared_ptr<Producer> producer1;
    DS_ASSERT_NOT_OK(CreateProducer(client1_, "stream1", 10 * MB, producer1));

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(CreateProducer(client1_, "stream1", 12 * MB, producer2));
}
}  // namespace st
}  // namespace datasystem

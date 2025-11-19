/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Unit test for stream cache on retain data feature
 */
#include <vector>
#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "common/stream_cache/element_generator.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "client/stream_cache/pub_sub_utils.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
constexpr int K_TEN = 10;
class RetainDataTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 1;
        opts.numWorkers = WORKER_COUNT;
        opts.vLogLevel = logLevel;
        opts.workerGflagParams += FormatString(" -node_timeout_s=%d -node_dead_timeout_s=%d -client_reconnect_wait_s=2",
                                               nodeTimeout, nodeDead);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        InitStreamClient(index, client);
        return Status::OK();
    }

    Status CreateConsumer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          const std::string &subName, std::shared_ptr<Consumer> &consumer)
    {
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        return client->Subscribe(streamName, config, consumer);
    }

    ProducerConf GetDefaultConf()
    {
        const int maxStreamSize = 10 * MB;
        ProducerConf conf;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        return conf;
    }

    void CheckCount(std::shared_ptr<StreamClient> client, const std::string &streamName, int producerCount,
                    int consumerCount)
    {
        uint64_t result = 0;
        if (producerCount >= 0) {
            DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, result));
            EXPECT_EQ(result, static_cast<uint64_t>(producerCount));
            result = 0;
        }
        if (consumerCount >= 0) {
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, result));
            EXPECT_EQ(result, static_cast<uint64_t>(consumerCount));
            result = 0;
        }
    }

    void CreateElement(size_t elementSize, Element &element, std::vector<uint8_t> &writeElement)
    {
        writeElement = RandomData().RandomBytes(elementSize);
        element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    }

    Status TryAndDeleteStream(std::shared_ptr<StreamClient> spClient, std::string streamName)
    {
        // if pending notifications retry delete
        Status rc = Status::OK();
        do {
            rc = spClient->DeleteStream(streamName);
            if (rc.IsError()) {
                sleep(K_TWO);
            }
        } while (rc.GetCode() == StatusCode::K_SC_STREAM_NOTIFICATION_PENDING);
        return rc;
    }

    const int nodeTimeout = 4;  // 4s;
    const int nodeDead = nodeTimeout * 3;
    const int waitNodeTimeout = nodeTimeout + 2;
    const int waitNodeDead = nodeDead + 4;
    const int logLevel = 2;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const uint32_t WORKER_COUNT = 3;
    const int DEFAULT_WAIT_TIME = 5000;
    const int WAIT_TIME = 1000;
    const int DEFAULT_NUM_ELEMENT = 100;
    const int SMALL_NUM_ELEMENT = 10;
    const size_t TEST_ELEMENT_SIZE = 4 * KB;
};

// We do not retain data when retainForNumConsumers == 0
// Should have no impact on existing flow
TEST_F(RetainDataTest, TestNotRetainData)
{
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::string streamName = "NotRetainData";
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 0;

    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < SMALL_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(SMALL_NUM_ELEMENT, WAIT_TIME, outElements));
    // No data received by late consumer
    ASSERT_EQ(outElements.size(), 0);
}

// We retain data when retainForNumConsumers > 0
TEST_F(RetainDataTest, TestRetainDataSPSCLocalConsumer)
{
    // Test data is retained when producers start to send before a local consumer is created
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;  // retain data until one consumer

    std::shared_ptr<Producer> producer;
    // Create producer and send data
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Create a late consumer
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    // Now should get the data
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer->Ack(size_t(DEFAULT_NUM_ELEMENT)));

    outElements.clear();
    // Create a 2nd late consumer
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config1("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config1, consumer2));
    // Data should be present as we only retain for one consumer
    // retainForNumConsumers == 1
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
}

TEST_F(RetainDataTest, TestRetainDataSPSCRemoteConsumer)
{
    // Test data is retained when producers start to send before a remote consumer is created
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;

    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer->Ack(size_t(DEFAULT_NUM_ELEMENT)));
    DS_ASSERT_OK(consumer->Close());

    outElements.clear();
    // Data should not be present as we only retain for one consumer
    // retainForNumConsumers == 1
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
}

TEST_F(RetainDataTest, TestRetainDataMPMC1)
{
    // Node1: Producer 1 Consumer1
    // Node2: Producer 2 Consumer2
    // retainForNumConsumers == 1
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));

    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));

    // Test config
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    // On node 1, Create producer1
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer1->Send(element));
    }

    // On node 2, Create producer2
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(streamName, producer2, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer2->Send(element));
    }

    // On node 1, Create Consumer1
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer1));

    // Get local producer data in on node 1
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Ack(size_t(DEFAULT_NUM_ELEMENT)));
    outElements.clear();

    // On node 2, Create Consumer2
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));

    // Get data on node 2
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT * K_TWO, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT) * K_TWO);
    outElements.clear();

    // Get remaining data from node 2
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Ack(size_t(DEFAULT_NUM_ELEMENT)));
}

TEST_F(RetainDataTest, TestRetainDataMPMC2)
{
    // Node1: Producer1 Consumer1
    // Node2: Consumer2
    // retainForNumConsumers == 2
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    // Remote can receive non of the elements before the expected num of consumers are all created
    ASSERT_EQ(outElements.size(), size_t(0));

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));

    // Now remote consumer can get the elements
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));

    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
}

TEST_F(RetainDataTest, TestRetainDataMPMC3)
{
    // Test that the node with no producer still gets to release pages
    // In other words, test the INIT state
    // Node1: Producer1 Consumer2
    // Node2: Consumer1 Consumer3
    // retainForNumConsumers == 2
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    // Remote can receive non of the elements before the expected num of consumers are all created
    ASSERT_EQ(outElements.size(), size_t(0));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Close());

    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    LOG(INFO) << "Acking element id " << outElements.back().id;
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));

    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config3, consumer3));
    DS_ASSERT_OK(consumer3->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
}

TEST_F(RetainDataTest, TestRetainDataCreateLocalConsumerFirst)
{
    // Test creating new local producers when subscriber already created
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    // Retain only for one consumer
    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;

    // Create that one consumer
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // Local producer should not have data retained
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // We should get data in remote
    std::shared_ptr<Consumer> consumer2;
    std::vector<Element> outElements;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(RetainDataTest, LEVEL1_TestRetainDataMPMC4)
{
    // Test that the node with no producer still gets to release pages
    // In other words, test that Subscribe does not set INIT to RETAIN
    // Node1: Producer1 Consumer3
    // Node2: Consumer1 Consumer2 Consumer4
    // retainForNumConsumers == 3
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    const int expectedNumConsumer = 3;
    conf.retainForNumConsumers = expectedNumConsumer;

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));

    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config3, consumer3));

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));

    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Ack(outElements.back().id));

    std::shared_ptr<Consumer> consumer4;
    SubscriptionConfig config4("sub4", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config4, consumer4));
    DS_ASSERT_OK(consumer4->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
}

TEST_F(RetainDataTest, TestCloseProducerWhileRetainData1)
{
    // Test that retained data can be received by local consumer after producer is closed
    // Create a producer with retainForNumConsumers == 1
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    // Close only producer
    DS_ASSERT_OK(producer->Close());

    // Create a local consumer
    // It should be able to get the retained data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer1));
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, TestCloseProducerWhileRetainData2)
{
    // Test that retained data can be received by remote consumer after producer is closed
    // Create a producer with retainForNumConsumers == 1
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    // Close only producer
    DS_ASSERT_OK(producer->Close());

    // Create a remote consumer
    // It should be able to get the retained data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer1));
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, TestCloseProducerWhileRetainData3)
{
    // Test that retained data can be received by remote consumer after producer is closed
    // Create a producer with retainForNumConsumers == 2
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::shared_ptr<StreamClient> client3;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(K_TWO, client3));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client3->Subscribe(streamName, config1, consumer1));

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 2;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < SMALL_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    // Close only producer
    DS_ASSERT_OK(producer->Close());

    // Create a remote consumer
    // It should be able to get the retained data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));

    // Get data from 2nd client
    outElements.clear();
    DS_ASSERT_OK(consumer2->Receive(SMALL_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(SMALL_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Close());

    // Get data from 3rd client
    outElements.clear();
    DS_ASSERT_OK(consumer1->Receive(SMALL_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(SMALL_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, TestDeleteWhileRetainDataSameNode)
{
    // Create a producer with retainForNumConsumers == 1
    // And Delete the stream
    // We should give higher priority to delete stream and allow it

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < SMALL_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(client1->DeleteStream(streamName));

    // Create a consumer
    // It should not get any data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer1));
    DS_ASSERT_OK(consumer1->Receive(SMALL_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, TestDeleteWhileRetainDataRemoteNode)
{
    // Create a producer with retainForNumConsumers == 1
    // And Delete the stream in a different node
    // We should give higher priority to delete stream and allow it

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    DS_ASSERT_OK(producer->Close());
    // Delete in a different node
    DS_ASSERT_OK(client2->DeleteStream(streamName));

    // Create a consumer
    // It should not get any data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer1));
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, LEVEL2_TestProducerCloseRemoteDeleteWhileRetainDataRemoteNode)
{
    // Create a consumer before producer
    // Then create a producer with retainForNumConsumers == 2
    // And Delete the stream in a different node
    // We should give higher priority to delete stream and allow it
    // Delayed ClearAllConsumers should be invoked and Flush is a no-op

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer1));

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 2;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer->Close());
    // Now close consumer sends async update topo notifications
    // So, we need to wait sometime before doing delete stream
    // Delete in a different node
    DS_ASSERT_OK(TryAndDeleteStream(client2, streamName));

    // Create a consumer
    // It should not get any data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer2->Close());
    // Sleep for auto-delete logic to go through
    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_WAIT_TIME));
}

TEST_F(RetainDataTest, LEVEL1_TestProducerCloseLocalDeleteWhileRetainDataRemoteNode)
{
    // Create a consumer before producer
    // Then create a producer with retainForNumConsumers == 2
    // And Delete the stream in the same node as producer
    // We should give higher priority to delete stream and allow it
    // Delayed ClearAllConsumers should be invoked and Flush is a no-op

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer1));

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 2;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer->Close());
    // Now close consumer sends async update topo notifications
    // So, we need to wait sometime before doing delete stream
    // Delete is called on the same node where ClearAllConsumer is delayed
    DS_ASSERT_OK(TryAndDeleteStream(client1, streamName));

    // Create a consumer
    // It should not get any data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer2->Close());
    // Sleep for auto-delete logic to go through
    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_WAIT_TIME));
}

TEST_F(RetainDataTest, TestAutoDeleteWhileRetainData1)
{
    // Create a producer
    // with retainForNumConsumers == 2
    // And AutoDelete enabled
    // We should give higher priority to Auto delete and allow it
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    // Shorten the delay for auto delete, so the auto delete goes through
    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "AutoCleanup.AdjustDelay", "call(3000)"));
    }

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;
    conf.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    // Close only producer
    DS_ASSERT_OK(producer->Close());

    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_WAIT_TIME));
    // Create a consumer
    // It should not get any data
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer1));
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer1->Close());
}

TEST_F(RetainDataTest, TestRetainDataSPMCConsumersComeAndGo)
{
    // In this test case,
    // we need to check if master restart
    // restores the state
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    // Config
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    // Create a new producer - this data should be retained
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Create a consumer - It should see data from producer
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config1, consumer1));
    // Try to receive data
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer1->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
    outElements.clear();

    // Create a new consumer on other node - It should see data from producer
    // Here current count == 1 but life time consumer count == 2
    outElements.clear();
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer2));
    // Try to receive data
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer2->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Close());
    outElements.clear();
}

// DFX testcases - Master crash
TEST_F(RetainDataTest, LEVEL1_TestMasterRestartWhileRetainData)
{
    // In this test case,
    // we need to check if master restart
    // restores the state
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    // Config
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;

    // Create a new producer - this data should be retained
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Now do a master restart - then check if retainData status remains same
    // Check if we have atleast a producer and no consumers
    CheckCount(client1, streamName, 1, 0);
    CheckCount(client2, streamName, 1, 0);
    // Restart the master
    cluster_->QuicklyShutdownWorker(1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    // Extend the sleep time for test case stability purposes
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    // Check if data is restored in the master
    CheckCount(client1, streamName, 1, 0);
    CheckCount(client2, streamName, 1, 0);

    // Create a new consumer on restarted node - It should see data from producer
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer2, consumer3;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer2));
    // Try to receive data
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer2->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer2->Close());
    outElements.clear();

    // As retainForNumConsumers==1 data should be gone now.
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer3));
    // Try to receive data
    DS_ASSERT_OK(consumer3->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer3->Close());
}

// Worker crash with consumer
TEST_F(RetainDataTest, TestConsumerWorkerCrashStopRemotePush)
{
    LOG(INFO) << "TestWorkerCrashStopRemotePush start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(K_TWO, client2));

    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    // Create a new producer - this data should be retain
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Worker Restart
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    // Node with consumers - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO));

    // new consumer should not get this data
    // Create a consumer - It should not see data from producer
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer1));
    // Try to receive data
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer1->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
    outElements.clear();

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));
    // Try to receive data
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer2->Close());
    outElements.clear();
}
// Worker crash with producer
TEST_F(RetainDataTest, LEVEL1_TestProducerWorkerCrashStopRemotePush)
{
    LOG(INFO) << "TestWorkerCrashStopRemotePush start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(K_TWO, client2));

    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = K_TWO;

    // Create a new producer - this data will be gone due to crash
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Worker Restart
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    // Node with consumers - crash happens
    cluster_->QuicklyShutdownWorker(0);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    // Create a new producer - this data should be retain
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer1->Send(element));
    }

    // new consumer should not get this data
    // Create a consumer - It should not see data from producer
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer1));

    // Existing consumer reads new data - But as its remote consumer it
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer->Close());
    outElements.clear();
    // Try to receive data
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer1->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
    outElements.clear();

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, consumer2));
    // Try to receive data
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer2->Close());
    outElements.clear();
}

TEST_F(RetainDataTest, TestCreateConsumerRollback)
{
    // Test that when notification fails in the middle for CreateConsumer and triggers rollback,
    // the retain data state on the producers are handled correctly
    // Node1: Producer 1
    // Node2: Producer 2
    // Node3: Consumer1
    // retainForNumConsumers == 1
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));
    std::shared_ptr<StreamClient> client3;
    DS_ASSERT_OK(InitClient(K_TWO, client3));

    // Test config
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;

    // On node 1, Create producer1
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer1->Send(element));
    }

    // On node 2, Create producer2
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(streamName, producer2, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer2->Send(element));
    }

    // On node 3, Create Consumer1, but fail with the second NotifyNewConsumer notification
    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i,
                                               "master.SubIncreaseNodeImpl.afterSendNotification",
                                               "1*return(K_RUNTIME_ERROR)"));
    }
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_NOT_OK(client3->Subscribe(streamName, config, consumer1));

    // On node 3, Re-Create Consumer1
    DS_ASSERT_OK(client3->Subscribe(streamName, config, consumer1));
    // Make sure we can get all the data
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT * K_TWO, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT) * K_TWO);
}

TEST_F(RetainDataTest, TestCreateProducerRollback)
{
    // Test that the retain data state in StreamManager is not set when CreateProducer fails and rolls back
    // Node1: Producer 1 Consumer1
    // Node2: Producer 2
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 1;

    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i,
                                               "master.PubIncreaseNodeImpl.beforeSendNotification",
                                               "1*return(K_RUNTIME_ERROR)"));
    }

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // The retain data state should remain in INIT for this test to pass
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamName, producer1, conf));

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(streamName, producer2, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer2->Send(element));
    }

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer->Ack(size_t(DEFAULT_NUM_ELEMENT)));
    DS_ASSERT_OK(consumer->Close());

    // Data should be released
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
}

TEST_F(RetainDataTest, DISABLED_TestResetWhileRetainData)
{
    std::vector<Element> outElements;
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::shared_ptr<StreamClient> client3;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(K_TWO, client3));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf = GetDefaultConf();
    conf.retainForNumConsumers = 3;

    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(CreateConsumer(client2, streamName, "sub1", consumer1));

    // Create a new producer - this data should be retain
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Create a consumer should not get any data yet
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateConsumer(client3, streamName, "sub2", consumer2));
    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);

    // After Reset and Resume there should be no data though retain data condition is met
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(CreateConsumer(client1, streamName, "sub3", consumer3));
    DS_ASSERT_OK(consumer3->Receive(DEFAULT_NUM_ELEMENT, WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);

    // Now let the producer add new data
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    DS_ASSERT_OK(producer->Close());

    // All three consumers should get this data
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer1->Close());

    DS_ASSERT_OK(consumer2->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer2->Close());

    DS_ASSERT_OK(consumer3->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer3->Close());
}
}  // namespace st
}  // namespace datasystem

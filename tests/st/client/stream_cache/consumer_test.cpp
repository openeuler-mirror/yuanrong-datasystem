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
#include "datasystem/common/encrypt/secret_manager.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "zmq_curve_test_common.h"
#include "common/stream_cache/element_generator.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "client/stream_cache/pub_sub_utils.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {

constexpr int K_2 = 2, K_5 = 5, K_10 = 10, K_20 = 20, K_100 = 100,
            K_1000 = 1000, K_5000 = 5000;

class ConsumerTest : public SCClientCommon {
public:
    explicit ConsumerTest(int pageSize = 4096) : pageSize_(pageSize)
    {
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
        defaultProducerConf_.pageSize = pageSize_;
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.workerGflagParams = "-v=2 -page_size=" + std::to_string(pageSize_);
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
            Element element(reinterpret_cast<uint8_t *>(&outData.front()), elementSize);
            ret.push_back(element);
        }
        return ret;
    }

    void SendHelper(std::shared_ptr<Producer> producer, Element element)
    {
        const int DEFAULT_SLEEP_TIME = 300;
        int retryLimit = 30;
        datasystem::Status rc = producer->Send(element);
        if (rc.IsError()) {
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
        }
        DS_ASSERT_OK(rc);
    }

    void ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements)
    {
        size_t remaining = numElements;
        int round = 0;
        const int roundLimit = 100;
        const int PER_RECEIVE_NUM = 500;
        const int DEFAULT_WAIT_TIME = 5000;
        while (remaining > 0 && round < roundLimit) {
            std::vector<Element> outElements;
            DS_ASSERT_OK(consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements));
            LOG(INFO) << "receive num : " << outElements.size() << " ;" << round++;
            if (!outElements.empty()) {
                remaining -= outElements.size();
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            }
        }
    }

    /**
     * @brief Creates a stream client at the given worker num and timeout
     * @param[in] workerNum The worker num to create the stream against
     * @param[in] timeout Timeout for RPC requests
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClient(int workerNum,  int32_t timeout, std::shared_ptr<StreamClient> &spClient)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerNum, workerAddress));
        // Create a client with user defined timeout
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = timeout };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        spClient = std::make_shared<StreamClient>(connectOptions);
        RETURN_IF_NOT_OK(spClient->Init());
        return Status::OK();
    }

protected:
    void InitTest()
    {
        InitStreamClient(0, client_);
    }
    std::shared_ptr<StreamClient> client_ = nullptr;
    uint64_t pageSize_;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

class ConsumerDataVerificationTest : public ConsumerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ConsumerTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_stream_data_verification=true ";
    }
};

TEST_F(ConsumerDataVerificationTest, SendRecvOneBigElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testSendRecvBigEle", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testSendRecvBigEle", config, consumer));

    RandomData rand;
    auto data = rand.GetRandomString(defaultProducerConf_.pageSize * 2);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_100, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
}

TEST_F(ConsumerDataVerificationTest, SendRecvOneSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testSendRecvSmallEle", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testSendRecvSmallEle", config, consumer));

    std::string data = "H";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_100, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
}

TEST_F(ConsumerTest, GetElementsWhenProducerClosesWithPage0)
{
    const int RECEIVE_WAIT_TIME = 19000;
    const int RECEIVE_TIME_COST = 12000;
    const int THREAD_WAIT_TIME = 1000;
    ThreadPool threadPool(1);

    threadPool.Submit([this]() {
        std::shared_ptr<StreamClient> cli;
        InitStreamClient(0, cli);

        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(client_->Subscribe("testRecvWhenProdCloseWithPg0", config, consumer));

        Timer timer;
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumer->Receive(RECEIVE_WAIT_TIME, outElements));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cost: " << timeCost;
        ASSERT_TRUE(timeCost < RECEIVE_TIME_COST);
        ASSERT_EQ(outElements.size(), size_t(1));
    });

    // wait a safer period to let the other thread call the Receive.
    std::this_thread::sleep_for(std::chrono::milliseconds(THREAD_WAIT_TIME));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testRecvWhenProdCloseWithPg0", producer, defaultProducerConf_));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer->Close());
}

TEST_F(ConsumerTest, LEVEL2_TestCreateConsumerLongTimeout)
{
    // Request should not timeout if client timeout is set to 10s and master takes more time

    // set timeout to 10 mins
    std::shared_ptr<StreamClient> client1;
    const int32_t timeoutMs = 1000 * 60 * 10;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);

    // Make master wait for 1 min and it should not timeout
    // We actually dont know who is the master so inject in both
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.Subscribe.wait", "1*sleep(60000)"));

    // This request should not timeout as client timeout is 10 mins.
    DS_ASSERT_OK(client1->Subscribe("CreateConLongTimeout", config, consumer));
}

TEST_F(ConsumerTest, LEVEL2_TestCloseConsumerLongTimeout)
{
    // Request should not timeout if client timeout is set to 10mins and master takes more time

    // set timeout to 10 mins
    std::shared_ptr<StreamClient> client1;
    const int32_t timeoutMs = 1000 * 60 * 10;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);

    // Make master wait for 1 min and it should not timeout
    // We actually dont know who is the master so inject in both
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.CloseConsumer.wait", "1*sleep(60000)"));

    // This request should not timeout as client timeout is 10 mins.
    DS_ASSERT_OK(client1->Subscribe("CloseConLongTimeout", config, consumer));
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ConsumerTest, EmptyCaseValidation)
{
    std::string workerip;
    int workerport = 0;

    // Test empty workerip is invalid (size 0 is invalid)
    LOG(INFO) << "workerip: " << workerip;
    LOG(INFO) << "workerport: " << workerport;
    std::shared_ptr<datasystem::StreamClient> client1;
    ConnectOptions options;
    options.host = workerip;
    options.port = workerport;
    client1 = std::make_shared<datasystem::StreamClient>(options);
    DS_ASSERT_NOT_OK(client1->Init());

    // Test invalid port number
    workerip = "0.0.0.0:0";
    LOG(INFO) << "workerip: " << workerip;
    LOG(INFO) << "workerport: " << workerport;
    std::shared_ptr<datasystem::StreamClient> client2;
    options.host = workerip;
    options.port = workerport;
    client2 = std::make_shared<datasystem::StreamClient>(options);
    DS_ASSERT_NOT_OK(client2->Init());
}

TEST_F(ConsumerTest, InvalidRecvParams)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("InvalidRecvParams", config, consumer));

    // Test invalid parameters
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(0, 0, outElements).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(consumer->Receive(0, K_10, outElements).GetCode(), StatusCode::K_INVALID);

    consumer->Close();
}

TEST_F(ConsumerTest, NoElementNoProducer)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("NoEleNoProducer", config, consumer));
    std::vector<Element> outElements;

    // Test no element to read
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));

    DS_ASSERT_OK(consumer->Receive(1, K_10, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    consumer->Close();
}

TEST_F(ConsumerTest, NoElementWithProducer)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("NoEleWithProducer", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("NoEleWithProducer", config, consumer));
    std::vector<Element> outElements;

    // Test no element to read
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    DS_ASSERT_OK(consumer->Receive(1, K_10, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    // Read the element
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));

    // No element to read when no blocking
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    // No element to read with blocking
    DS_ASSERT_OK(consumer->Receive(1, K_10, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    consumer->Close();
}

TEST_F(ConsumerTest, RollbackInvalidElement)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testRollbackInvalidEle", config, consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testRollbackInvalidEle", producer, defaultProducerConf_));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    Element elementInvalid(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // 1. send the invalid element
    datasystem::inject::Set("HugeMemoryCopy", "1*return(K_RUNTIME_ERROR)");
    DS_ASSERT_NOT_OK(producer->Send(elementInvalid));
    datasystem::inject::Clear("HugeMemoryCopy");

    const uint32_t timeoutMs = 1000;
    std::vector<Element> outElements;
    // 2. expect that the invalid element will not be received
    DS_ASSERT_OK(consumer->Receive(1, timeoutMs, outElements));
    ASSERT_TRUE(outElements.size() == 0);
}

TEST_F(ConsumerTest, NoExpectedElementNum)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testNoExpectedEleNum";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    // Test no element to read
    DS_ASSERT_OK(consumer->Receive(0, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    DS_ASSERT_OK(consumer->Receive(K_10, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    std::string data1 = "Hello World1";
    std::string data2 = "Hello World2";
    Element element1(reinterpret_cast<uint8_t *>(&data1.front()), data1.size());
    DS_ASSERT_OK(producer->Send(element1));
    Element element2(reinterpret_cast<uint8_t *>(&data2.front()), data2.size());
    DS_ASSERT_OK(producer->Send(element2));
    // Read one element out of two. Keep the remaining element in client queue.
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    // Using the Receive API with no expectNum, all elements already stored in the client local queue can be read.
    // Producer pushes 2 elements, 1 already read, 1 in local queue. Read the element from local queue.
    DS_ASSERT_OK(consumer->Receive(0, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));
    std::string actualData2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data2, actualData2);
    // Timeout of 0 does not wait for more elements to come. Local queue is empty. Client gets nothing.
    DS_ASSERT_OK(consumer->Receive(0, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));

    std::string data3 = "Hello World3";
    std::string data4 = "Hello World4";
    Element element3(reinterpret_cast<uint8_t *>(&data3.front()), data3.size());
    DS_ASSERT_OK(producer->Send(element3));
    Element element4(reinterpret_cast<uint8_t *>(&data4.front()), data4.size());
    DS_ASSERT_OK(producer->Send(element4));

    // As expectNum is not set, receive both elements sent by the producer.
    DS_ASSERT_OK(consumer->Receive(K_10, outElements));
    ASSERT_EQ(outElements.size(), size_t(K_2));
    std::string actualData3(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data3, actualData3);
    std::string actualData4(reinterpret_cast<char *>(outElements[1].ptr), outElements[1].size);
    EXPECT_EQ(data4, actualData4);
}

TEST_F(ConsumerTest, DISABLED_ReceiveTimeoutOverwriteRpcTimeout)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("test1", config, consumer));

    DS_ASSERT_OK(inject::Set("client.StreamReceive.overwriteRpcTimeout", "call()"));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, UINT32_MAX, outElements), Status::OK());
}

TEST_F(ConsumerTest, ConsumerCloseSecondConsumerRecv)
{
    // Create First Producer
    std::shared_ptr<Producer> producer;
    const uint64_t maxStreamSize = 1024*1024;
    defaultProducerConf_.maxStreamSize = maxStreamSize;
    std::string streamName = "OneConCloseOtherConRecv";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    // Create First Consumer
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    const int cacheCapacity = 192;
    config.cacheCapacity = cacheCapacity; // Have a low value for cache
    // Switch on the AutoAck
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer, true));

    // Generate data until cache is full
    int elementCount = 0;
    std::string data;
    const uint64_t numElements = 1024;
    const uint64_t size = 1024;
    std::vector<Element> elements = GenerateElements(numElements, size, data);
    for (auto &element : elements) {
        Status rc = producer->Send(element);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            break;
        } else if (rc.IsOk()) {
            ++elementCount;
        }
    }

    // Get all the data and close
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(elementCount, K_100, outElements));
    DS_ASSERT_OK(consumer->Close());

    // Create another consumer
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub2", SubscriptionType::STREAM);
    config1.cacheCapacity = cacheCapacity;
    // Switch on the AutoAck
    DS_ASSERT_OK(client_->Subscribe(streamName, config1, consumer1, true));

    elementCount = 0;
    sleep(K_2); // wait for producer to get unblocked
    //  Send data to stream until we get OOM
    for (auto &element : elements) {
        Status rc = producer->Send(element);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            break;
        } else if (rc.IsOk()) {
            ++elementCount;
        }
    }
    // Start Receiving elements now, this should Ack and clear all elements
    outElements.clear();
    DS_ASSERT_OK(consumer1->Receive(K_10, K_100, outElements));
    outElements.clear();
    DS_ASSERT_OK(consumer1->Receive(K_10, K_100, outElements));

    // Now producer should have space to take atleast an element
    sleep(K_2); // wait for producer to get unblocked
    DS_ASSERT_OK(producer->Send(elements[0]));
    DS_ASSERT_OK(producer->Send(elements[0]));
    DS_ASSERT_OK(producer->Send(elements[0]));
    DS_ASSERT_OK(producer->Send(elements[0]));
}

TEST_F(ConsumerTest, SendRecvOneSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("SendRecvSmallEle", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("SendRecvSmallEle", config, consumer));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_100, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
}

TEST_F(ConsumerTest, TestAutoAck1)
{
    // Test that auto ack works for new consumers
    const int DEFAULT_WAIT_TIME = 5000;
    const int DEFAULT_ELEMENT_SIZE = 180;
    const bool ENABLE_AUTO_ACK = true;
    std::shared_ptr<Producer> producer1;
    std::string streamName = "testAutoAck1";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer1, ENABLE_AUTO_ACK));
    const int elementNum = 10000;

    auto func = [](std::shared_ptr<Producer> &producer, std::shared_ptr<Consumer> &consumer) {
        std::thread producerThrd([&producer]() {
            std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
            Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
            for (int i = 0; i < elementNum; i++) {
                DS_ASSERT_OK(producer->Send(element));
            }
            DS_ASSERT_OK(producer->Close());
        });
        std::vector<Element> outElements;
        int received = 0;
        while (received < elementNum) {
            DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
            ASSERT_EQ(outElements.size(), 1);
            received++;
        }
        DS_ASSERT_OK(consumer->Close());
        producerThrd.join();
    };

    // First round of produce and consumer
    func(producer1, consumer1);

    // Create new producer and consumer, for a second round
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config2, consumer2, ENABLE_AUTO_ACK));
    // Make sure here it does not see residue elements
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer2->Receive(1, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer2, defaultProducerConf_));

    // Second round of produce and consumer
    func(producer2, consumer2);

    // Create new consumer only
    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config3, consumer3, ENABLE_AUTO_ACK));

    // Make sure here it does not see residue elements
    DS_ASSERT_OK(consumer3->Receive(1, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
}

TEST_F(ConsumerTest, TestAutoAck2)
{
    // Test that auto ack works even if the next receive gets no elements
    const int DEFAULT_WAIT_TIME = 5000;
    const int DEFAULT_ELEMENT_SIZE = 500 * KB;
    const int DEFAULT_MAX_STREAM_SIZE = 2 * MB;
    const bool ENABLE_AUTO_ACK = true;
    std::string streamName = "AutoAck2Test";
    std::shared_ptr<Producer> producer1;
    ProducerConf conf;
    conf.maxStreamSize = DEFAULT_MAX_STREAM_SIZE;
    conf.pageSize = 1 * MB;
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer1, conf));
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer1, ENABLE_AUTO_ACK));
    const int elementNum = 10;

    std::thread producerThrd([this, &producer1]() {
        std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        for (int i = 0; i < elementNum; i++) {
            SendHelper(producer1, element);
        }
        DS_ASSERT_OK(producer1->Close());
    });
    std::vector<Element> outElements;
    int received = 0;
    const int MAX_EXPECT_NUM = DEFAULT_MAX_STREAM_SIZE / DEFAULT_ELEMENT_SIZE;
    while (received < elementNum) {
        DS_ASSERT_OK(consumer1->Receive(MAX_EXPECT_NUM, DEFAULT_WAIT_TIME, outElements));
        LOG(INFO) << "Received element num: " << outElements.size();
        ASSERT_GT(outElements.size(), 0);
        received += outElements.size();
    }
    DS_ASSERT_OK(consumer1->Close());
    producerThrd.join();
}

TEST_F(ConsumerTest, DISABLED_SendRecvOneSmallElementWithTwoFlush)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("test1", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("test1", config, consumer));
    const int K_4 = 4;

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(K_4, K_1000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);

    std::string data2 = "Test";
    Element element2(reinterpret_cast<uint8_t *>(&data2.front()), data2.size());
    DS_ASSERT_OK(producer->Send(element2));
    outElements.clear();
    ASSERT_EQ(consumer->Receive(K_4, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(K_2));
    std::string actualData2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data2, actualData2);
}

TEST_F(ConsumerTest, TestReceiveProducerIdle)
{
    // Test consumer receive when a producer is idle
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    const int numElement = 5000;
    size_t testSize = 4 * KB;
    std::vector<uint8_t> writeElement;
    writeElement = RandomData().RandomBytes(testSize);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    const int maxStreamSize = 10 * MB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    std::shared_ptr<Producer> producer;
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, conf));
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer2, conf));
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));

    std::thread producerThrd([&]() {
        for (int i = 0; i < numElement; i++) {
            SendHelper(producer, element);
        }
        for (int i = 0; i < numElement + numElement; i++) {
            SendHelper(producer2, element);
        }
    });

    ReceiveHelper(consumer, numElement + numElement + numElement);

    producerThrd.join();
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client_->DeleteStream(streamName));
}

TEST_F(ConsumerTest, OneSubMultiConsumers1)
{
    // Create one producer and one consumer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testOSMC1", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testOSMC1", config, consumer));

    // Write data that less than one page
    uint64_t elementSize = 8;
    uint64_t onePageElementNum = pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i < onePageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }
    // Receive data that less than one page
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(onePageElementNum - 1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum - 1);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum - 1);
    ASSERT_EQ(consumer->Ack(outElements.back().id), Status::OK());
}

TEST_F(ConsumerTest, OneSubMultiConsumers2)
{
    // Create one producer and one consumer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testOSMC2", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testOSMC2", config, consumer));

    // Write less than two page
    uint64_t elementSize = 8;
    uint64_t twoPageElementNum = 2 * pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i < twoPageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }
    // Receive one page data and ack
    uint64_t onePageElementNum = twoPageElementNum / K_2;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(consumer->Ack(outElements.back().id), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);
}

TEST_F(ConsumerTest, OneSubMultiConsumers3)
{
    // Create one producer and two consumer
    std::string streamName("OneSubMultiConsumers3");
    std::vector<std::string> subNameList{ "sub1", "sub2", "sub3" };
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1, consumer2;
    SubscriptionConfig config1(subNameList[0], SubscriptionType::STREAM);
    SubscriptionConfig config2(subNameList[1], SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config1, consumer1));
    DS_ASSERT_OK(client_->Subscribe(streamName, config2, consumer2));

    // Write two page data
    uint64_t elementSize = 8;
    uint64_t twoPageElementNum = K_2 * pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i <= twoPageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }

    // Consumer1 Receive one page data and ack
    uint64_t onePageElementNum = twoPageElementNum / K_2;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(consumer1->Ack(outElements.back().id), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);

    // Consumer2 Receive one page data but not ack
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);

    // Create consumer3 and recv data, it should can receive all the data
    LOG(INFO) << "Start to create consumer3.";
    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3(subNameList[2], SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config3, consumer3));

    outElements.clear();
    ASSERT_EQ(consumer3->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);
}

TEST_F(ConsumerTest, RecvWithCache)
{
    std::shared_ptr<Producer> producer;
    int dataNum = 21;
    std::string stream0{ "RecvWithCache" };
    ProducerConf conf;
    conf.delayFlushTime = 10;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    DS_ASSERT_OK(client_->CreateProducer(stream0, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub0", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(stream0, config, consumer));

    std::vector<std::string> dataList;
    for (int i = 0; i < dataNum; ++i) {
        std::string data = "Test-Data" + std::to_string(i);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));
        dataList.emplace_back(data);
    }
    std::vector<Element> outElements;
    uint64_t cursor = 1;
    Timer timer;
    while (timer.ElapsedSecond() <= 1) {
        std::vector<Element> oneTimeOutElements;
        consumer->Receive(K_5, K_100, oneTimeOutElements);
        if (!oneTimeOutElements.empty()) {
            LOG(INFO) << FormatString("Receive %d element", oneTimeOutElements.size());
            for (int i = 0; i < static_cast<int>(oneTimeOutElements.size()); ++i) {
                ASSERT_EQ(oneTimeOutElements[i].id, cursor + i);
            }
            outElements.insert(outElements.end(), oneTimeOutElements.begin(), oneTimeOutElements.end());
            cursor += oneTimeOutElements.size();
        }
    }
    ASSERT_EQ(outElements.size(), static_cast<size_t>(dataNum));
    ASSERT_EQ(outElements[0].id, size_t(1));
    for (int i = 0; i < dataNum; ++i) {
        std::string receivedData(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
        EXPECT_EQ(dataList[i], receivedData);
        LOG(INFO) << FormatString("No:%d, Data:%s", i, receivedData);
    }
}

TEST_F(ConsumerTest, MultiSubsMultiConsumers)
{
    // Create one producer and two subscription with two consumer firstly
    std::string streamName("MultiSubMultiCon");
    std::string sub1("sub1");
    std::string sub2("sub2");
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(client_->Subscribe(streamName, SubscriptionConfig(sub1, SubscriptionType::STREAM), consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client_->Subscribe(streamName, SubscriptionConfig(sub2, SubscriptionType::STREAM), consumer2));

    // Write two page elements
    uint64_t elementSize = 8;
    uint64_t twoPageElementNum = K_2 * pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i <= twoPageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }

    // Consumer1 Receive one page data and ack
    uint64_t onePageElementNum = twoPageElementNum / K_2;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(consumer1->Ack(outElements.back().id), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);

    // Consumer2 Receive one page data but not ack
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(onePageElementNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), onePageElementNum);
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, onePageElementNum);
}

TEST_F(ConsumerTest, SingleConsumerReceiveNotEnoughElementAfterTimeout)
{
    // Create one producer and one subscription two consumer firstly
    std::string streamName("SingleConRecvNotEnough");
    std::string sub1("sub0");
    std::unordered_map<std::string, InputStreamInfo> info;
    info[streamName].producerNum = 1;
    info[streamName].subscriptions[sub1] = std::make_pair(SubscriptionType::STREAM, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    DS_ASSERT_OK(CreateProducersAndConsumers(info, output));

    // Write elements
    uint64_t elementSize = 8;
    int elementNum = 10;
    std::string data = RandomData().GetRandomString(K_100 * elementSize);
    for (int i = 1; i <= elementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(output[streamName].producers[0]->Send(element), Status::OK());
    }

    // Consumer1 want recv 20 elements at the same time but only get 10 after timeout
    Consumer *consumer = output[streamName].consumers[sub1][0].get();
    int expectNum = 20;
    std::vector<Element> outElements;
    LOG(INFO) << FormatString("Consumer Start recv %d elements.", expectNum);
    ASSERT_EQ(consumer->Receive(expectNum, K_1000, outElements), Status::OK());
    LOG(INFO) << FormatString("Consumer Received %d elements.", outElements.size());
    ASSERT_EQ(consumer->Ack(outElements.back().id), Status::OK());
    ASSERT_EQ(outElements.size(), static_cast<size_t>(elementNum));
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, static_cast<size_t>(elementNum));
}

TEST_F(ConsumerTest, MultiConsumerReceiveNotEnoughElementAfterTimeout)
{
    // Create one producer and one subscription two consumer firstly
    std::string streamName("MultiConRecvNotEnough");
    std::string sub1("sub0");
    std::string sub2("sub1");
    std::unordered_map<std::string, InputStreamInfo> info;
    info[streamName].producerNum = 1;
    info[streamName].subscriptions[sub1] = std::make_pair(SubscriptionType::STREAM, 1);
    info[streamName].subscriptions[sub2] = std::make_pair(SubscriptionType::STREAM, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    DS_ASSERT_OK(CreateProducersAndConsumers(info, output));

    // Write elements
    uint64_t elementSize = 8;
    int elementNum = 10;
    std::string data = RandomData().GetRandomString(K_100 * elementSize);
    for (int i = 1; i <= elementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(output[streamName].producers[0]->Send(element), Status::OK());
    }

    // Consumer1 and Consumer2 want recv 20 elements at the same time but only get 10 after timeout
    std::vector<std::thread> threads;
    for (size_t i = 0; i < output[streamName].consumers.size(); i++) {
        threads.emplace_back([streamName, elementNum, i, &output]() {
            auto subName = "sub" + std::to_string(i);
            Consumer *consumer = output[streamName].consumers[subName][0].get();
            int expectNum = 20;
            std::vector<Element> outElements;
            LOG(INFO) << FormatString("Consumer %d Start recv %d elements.", i, expectNum);
            ASSERT_EQ(consumer->Receive(expectNum, K_1000, outElements), Status::OK());
            LOG(INFO) << FormatString("Consumer %d Received %d elements.", i, outElements.size());
            ASSERT_EQ(outElements.size(), static_cast<size_t>(elementNum));
            ASSERT_EQ(outElements.front().id, size_t(1));
            ASSERT_EQ(outElements.back().id, static_cast<size_t>(elementNum));
        });
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
}

TEST_F(ConsumerTest, ReceiveEnoughElementBeforeTimeout)
{
    // Create one producer and one subscription two consumer firstly
    std::string streamName("RecvEleBeforeTimeout");
    std::string sub1("sub0");
    std::string sub2("sub1");
    std::unordered_map<std::string, InputStreamInfo> info;
    info[streamName].producerNum = 1;
    info[streamName].subscriptions[sub1] = std::make_pair(SubscriptionType::STREAM, 1);
    info[streamName].subscriptions[sub2] = std::make_pair(SubscriptionType::STREAM, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    DS_ASSERT_OK(CreateProducersAndConsumers(info, output));

    // Write elements
    uint64_t elementSize = 8;
    int elementNum = 10;
    std::string data = RandomData().GetRandomString(K_100 * elementSize);
    for (int i = 1; i <= elementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(output[streamName].producers[0]->Send(element), Status::OK());
    }
    LOG(INFO) << "Start to flush first 10 elements.";

    // Consumer1 and Consumer2 want recv 20 elements at the same time And get 20 before timeout
    std::vector<std::thread> threads;
    for (size_t i = 0; i < output[streamName].consumers.size(); i++) {
        threads.emplace_back([streamName, i, &output]() {
            auto subName = "sub" + std::to_string(i);
            Consumer *consumer = output[streamName].consumers[subName][0].get();
            int expectNum = K_20;
            std::vector<Element> outElements;
            LOG(INFO) << FormatString("Consumer %d Start recv %d elements.", i, expectNum);
            ASSERT_EQ(consumer->Receive(expectNum, K_5000, outElements), Status::OK());
            LOG(INFO) << FormatString("Consumer %d Received %d elements.", i, outElements.size());
            ASSERT_EQ(outElements.size(), static_cast<size_t>(expectNum));
            ASSERT_EQ(outElements.front().id, size_t(1));
            ASSERT_EQ(outElements.back().id, size_t(K_20));
            ASSERT_EQ(outElements.back().id, static_cast<size_t>(expectNum));
        });
    }
    sleep(K_2);
    for (int i = 1; i <= elementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(output[streamName].producers[0]->Send(element), Status::OK());
    }
    LOG(INFO) << "Start to flush second 10 elements.";
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
}

TEST_F(ConsumerTest, InvalidAck)
{
    std::string streamName("TestInvalidAck");
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1, consumer2;
    ASSERT_EQ(client_->CreateProducer(streamName, producer, defaultProducerConf_), Status::OK());
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config1, consumer1));

    uint64_t elementSize = 8;
    int elementNum = 10;
    int invalidAckNum = 20;
    std::string data = RandomData().GetRandomString(K_100 * elementSize);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
    for (int i = 1; i <= elementNum; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer1->Receive(elementNum, K_1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(elementNum));
    DS_ASSERT_NOT_OK(consumer1->Ack(invalidAckNum));
    DS_ASSERT_OK(consumer1->Ack(elementNum));
    DS_ASSERT_OK(consumer1->Close());

    outElements.clear();
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config2, consumer2));
    DS_ASSERT_OK(consumer2->Receive(K_100, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
}

class DefaultPageTest : public ConsumerTest {
public:
    DefaultPageTest() : ConsumerTest(1024 * 1024)
    {
    }
};

TEST_F(DefaultPageTest, InvalidAck2)
{
    LOG(INFO) << "Producer send multi small element(element smaller than the 1/16 of buffer).";
    // create pub
    std::string streamName = "stream_001_01";
    std::shared_ptr<Producer> producer;
    ASSERT_EQ(client_->CreateProducer(streamName, producer, defaultProducerConf_), Status::OK());

    LOG(INFO) << "create producer successfully.";
    // create sub and consumer
    std::string subName = "stream_001_01_sub_03";
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    ASSERT_EQ(client_->Subscribe(streamName, config, consumer), Status::OK());
    LOG(INFO) << "Create consumer successfully.";
    // generate random buffer
    size_t testSize1 = 4096;
    std::string writeBuffer = RandomData().GetRandomString(testSize1);
    int elementNum = 257;
    const int K_157 = 157, K_158 = 158;
    for (int i = 0; i < elementNum; i++) {
        // Write and flush one element
        Element element(reinterpret_cast<uint8_t *>(&writeBuffer.front()), writeBuffer.size());
        ASSERT_EQ(producer->Send(element), Status::OK());
    }

    // Consumer Receive
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(K_157, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(K_157));
    ASSERT_EQ(outElements.front().id, size_t(1));
    ASSERT_EQ(outElements.back().id, size_t(K_157));

    // Producer Close
    ASSERT_EQ(producer->Close(), Status::OK());
    LOG(INFO) << "Close producer successfully.";

    // ack
    ASSERT_EQ(consumer->Ack(K_158).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(consumer->Ack(K_157), Status::OK());
    std::vector<Element> outElements2;
    ASSERT_EQ(consumer->Receive(K_100, 0, outElements2), Status::OK());
}

TEST_F(ConsumerTest, DelayFlushSendRecvOneSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("auto_flush_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("auto_flush_test", config, consumer));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_10, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    LOG(INFO) << "receive data is :" << actualData;
    ASSERT_EQ(data, actualData);
}

TEST_F(ConsumerTest, SendElementAndAutoFlushWithoutDelay)
{
    ProducerConf conf;
    conf.delayFlushTime = 0;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("auto_no_delay_test", producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("auto_no_delay_test", config, consumer));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_100, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
}

TEST_F(ConsumerTest, ContinuousSendRecvSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("continuous_send_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("continuous_send_test", config, consumer));

    std::string data[K_10];
    for (int i = 0; i < K_10; i++) {
        data[i] = "Hello World" + std::to_string(i);
        uint8_t id = i + 1;
        Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
        element.id = id;
        DS_ASSERT_OK(producer->Send(element));
    }
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(K_10, K_1000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(K_10));
    for (int i = 0; i < K_10; i++) {
        ASSERT_EQ(outElements[i].id, static_cast<size_t>(i + 1));
        std::string actualData(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
        ASSERT_EQ(data[i], actualData);
    }
}

TEST_F(ConsumerTest, SendElementWhenFlashSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("sametime_send_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("sametime_send_test", config, consumer));
    const int K_15 = 15;
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    std::string data2 = "Test";
    Element element2(reinterpret_cast<uint8_t *>(&data2.front()), data2.size());
    DS_ASSERT_OK(producer->Send(element));
    std::this_thread::sleep_for(std::chrono::milliseconds(K_5));
    DS_ASSERT_OK(producer->Send(element2));

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(K_2, K_15, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(K_2));
    ASSERT_EQ(outElements[0].id, size_t(1));
    ASSERT_EQ(outElements[1].id, size_t(K_2));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
    std::string actualData2(reinterpret_cast<char *>(outElements[1].ptr), outElements[1].size);
    EXPECT_EQ(data2, actualData2);
}

TEST_F(ConsumerTest, FlashAgainWhenAutoFlashSmallElement)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("sametime_flush_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("sametime_flush_test", config, consumer));
    const int K_5 = 5;

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::this_thread::sleep_for(std::chrono::milliseconds(K_5));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    ASSERT_EQ(data, actualData);
}

TEST_F(ConsumerTest, ReceiveCacheTest)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("cache_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("cache_test", config, consumer));

    std::string data = "a";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    for (int i = 0; i < K_20; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    std::vector<Element> outElements;
    for (int i = 0; i < K_20; i++) {
        outElements.clear();
        ASSERT_EQ(consumer->Receive(1, K_1000, outElements), Status::OK());
        ASSERT_EQ(outElements.size(), size_t(1));
        std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
        ASSERT_EQ(data, actualData);
        outElements.clear();
    }

    // Receive expect greater than cache. Should be no more elements to recv
    DS_ASSERT_OK(consumer->Receive(1, K_1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
}

TEST_F(ConsumerTest, TestInfiniteWaitRecv)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("infinite_wait_test", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("infinite_wait_test", config, consumer));

    auto producerThread = std::make_unique<std::thread>([&producer]() {
        size_t testSize1 = 4096;
        std::string writeBuffer = RandomData().GetRandomString(testSize1);
        sleep(K_5);
        Element element(reinterpret_cast<uint8_t *>(&writeBuffer.front()), writeBuffer.size());
        ASSERT_EQ(producer->Send(element), Status::OK());
    });

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, -1, outElements));
    producerThread->join();
}

TEST_F(ConsumerTest, TestSpecialOrder)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "specialOrderTest";
    const int K_2 = 2;

    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));
    // after worker write data to client, client send the next receive before worker remove the pending receive.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.stream.after_send_pending", "sleep(500)"));
    // worker execute the pending receive timer before add the pending receive task.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.stream.before_add_pending", "sleep(500)"));

    for (int i = 0; i < K_2; i++) {
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumer->Receive(K_10, 1, outElements));
    }
}

TEST_F(ConsumerTest, GetStatisticsMessage1)
{
    // Create one producer and one consumer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testGetStatMsg1", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testGetStatMsg1", config, consumer));

    // Write data that less than one page
    uint64_t elementSize = 8;
    uint64_t onePageElementNum = pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i < onePageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(onePageElementNum - 1, 0, outElements), Status::OK());
    uint64_t recEle;
    uint64_t notProcEle;
    consumer->GetStatisticsMessage(recEle, notProcEle);
    ASSERT_EQ(recEle, outElements.size());
    ASSERT_EQ(notProcEle, recEle);
    ASSERT_EQ(consumer->Ack(outElements.back().id), Status::OK());
    consumer->GetStatisticsMessage(recEle, notProcEle);
    ASSERT_EQ(recEle, outElements.size());
    ASSERT_EQ(notProcEle, 0u);
}

TEST_F(ConsumerTest, GetStatisticsMessage2)
{
    // Create one producer and one consumer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testGetStatMsgTwo", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testGetStatMsgTwo", config, consumer));

    // Write data that less than one page
    uint64_t elementSize = 8;
    uint64_t onePageElementNum = pageSize_ / elementSize;
    std::string data = RandomData().GetRandomString(pageSize_);
    for (uint64_t i = 1; i < onePageElementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), elementSize);
        ASSERT_EQ(producer->Send(element), Status::OK());
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(onePageElementNum - 1, 0, outElements), Status::OK());
    uint64_t recEle;
    uint64_t notProcEle;
    consumer->GetStatisticsMessage(recEle, notProcEle);
    ASSERT_EQ(recEle, outElements.size());
    ASSERT_EQ(notProcEle, recEle);

    for (auto ele : outElements) {
        ASSERT_EQ(consumer->Ack(ele.id), Status::OK());
        consumer->GetStatisticsMessage(recEle, notProcEle);
        ASSERT_EQ(recEle, outElements.size());
        ASSERT_EQ(notProcEle, recEle - ele.id);
    }
}


TEST_F(ConsumerTest, GetStatisticsMessage3)
{
    // Test GetStatistics with AutoAck and more than one page
    // Create one producer and one consumer
    std::shared_ptr<Producer> producer;
    ProducerConf conf = {.delayFlushTime = 5, .pageSize = K_2 * K_2 * KB};
    DS_ASSERT_OK(client_->CreateProducer("testGetStatMsgThree", producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testGetStatMsgThree", config, consumer, true));

    const int DEFAULT_WAIT_TIME = 5000;
    uint64_t recEle, notProcEle;
    std::string data = RandomData().GetRandomString(1 * KB);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    data = RandomData().GetRandomString(1);
    Element element2(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    consumer->GetStatisticsMessage(recEle, notProcEle);
    ASSERT_EQ(recEle, 0);
    ASSERT_EQ(notProcEle, 0);
    // producer sends data (3.5 pages of elements)
    // First 3 pages have 3x 1KB elements, 4th page has 1x 1KB, 1000x 1B elements
    for (int i = 0; i < K_10; i++) {
        SendHelper(producer, element);
    }
    for (int i = 0; i < K_1000; i++) {
        SendHelper(producer, element2);
    }

    std::vector<Element> outElements;
    for (int i = 0; i < K_10 + K_1000; i++) {
        DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
        ASSERT_EQ(outElements.size(), 1);
    }
    // Last Receive to trigger auto-ack on last element
    DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), 0);
    consumer->GetStatisticsMessage(recEle, notProcEle);
    ASSERT_EQ(recEle, K_10 + K_1000);
    ASSERT_EQ(notProcEle, 0);
}


class SCClientZmqCurveTest : public ConsumerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = workerCount;
        opts.enableLivenessProbe = true;
        ConsumerTest::SetClusterSetupOptions(opts);
        // use default configurations for all the other zmq curve gflags settings
        opts.numEtcd = 1;
        opts.vLogLevel = defaultLogLevel;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    Status InitTest()
    {
        InitStreamClient(0, client1_);
        InitStreamClient(1, client2_);
        return Status::OK();
    }
    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    const uint32_t workerCount = 2;
    const uint32_t defaultLogLevel = 3;
};

/*
On same node. Producer created before consumer. Consumer calls receive before producer send.
Consumer should be able to receive all data from producer.
*/
TEST_F(ConsumerTest, ReceiveThenSend)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testRecvThenSend", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testRecvThenSend", config, consumer));

    int timeOut = 10000;
    std::vector<Element> outElements;
    std::thread receiveThread([&]() { ASSERT_EQ(consumer->Receive(1, timeOut, outElements), Status::OK()); });
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    sleep(1);
    receiveThread.join();
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
}

/*
On same node. Producer created and calls send before consumer creation. Consumer should not
be able to receive any data because of late create/subscribe.
*/
TEST_F(ConsumerTest, SendBeforeConsumerCreate)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("SendBeforeConCreate", producer, defaultProducerConf_));
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    int timeOut = 100;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("SendBeforeConCreate", config, consumer));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, timeOut, outElements), Status::OK());
    // Cannot receive sent element when Consumer create after send
    ASSERT_EQ(outElements.size(), size_t(0));
}

/*
On same node. Consumer created before producer. Producer sends data before consumer receive.
Consumer should be able to receive all data from producer.
*/
TEST_F(ConsumerTest, CreateConsumerBeforeProducer)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("CreateConBeforeProd", config, consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("CreateConBeforeProd", producer, defaultProducerConf_));

    int timeOut = 100;
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, timeOut, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
}

/*
On same node. Consumer created before producer. Consumer receives before producer sends data.
Consumer should be able to receive all data from producer.
*/
TEST_F(ConsumerTest, CreateConsumerFirstReceiveThenSend)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testRecvBeforeSend", config, consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testRecvBeforeSend", producer, defaultProducerConf_));

    int timeOut = 10000;
    std::vector<Element> outElements;
    std::thread receiveThread([&]() { ASSERT_EQ(consumer->Receive(1, timeOut, outElements), Status::OK()); });
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    sleep(1);
    receiveThread.join();
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
}

TEST_F(ConsumerTest, TestDoubleClose)
{
    // Test that if CloseConsumer RPC fails in ConsumerImpl::Close(),
    // the implicit Close triggered by the destructor will not double release the page.
    const int maxStreamSize = 10 * MB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testDoubleClose", producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testDoubleClose", config, consumer));

    const size_t DEFAULT_ELEMENT_SIZE = 500 * KB;
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_1000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    // Call close and expect the injected failure
    datasystem::inject::Set("ConsumerImpl.CloseConsumerRPC.Fail", "1*return(K_RPC_UNAVAILABLE)");
    DS_ASSERT_NOT_OK(consumer->Close());
    consumer.reset();
    const int numElement = 10;
    for (int i = 0; i < numElement; i++) {
        SendHelper(producer, element);
    }
}

TEST_F(ConsumerTest, TestIdempotentClose)
{
    // Test that if producer/consumer is closed, calling Close() again will return OK
    const int maxStreamSize = 10 * MB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("testIdempotentClose", producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testIdempotentClose", config, consumer));

    const size_t DEFAULT_ELEMENT_SIZE = 500 * KB;
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, K_1000, outElements), Status::OK());
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));
    ASSERT_EQ(outElements.size(), size_t(1));
    // First close
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    // Other methods should return error
    DS_ASSERT_NOT_OK(producer->Send(element));
    DS_ASSERT_NOT_OK(consumer->Receive(1, K_1000, outElements));
    DS_ASSERT_NOT_OK(consumer->Ack(outElements.back().id));
    // Second close should return OK
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
}

class SPMCTest : public ConsumerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = numWorkers;
        opts.numEtcd = numEtcd;
        opts.numRpcThreads = numRpcThreads;
        SCClientCommon::SetClusterSetupOptions(opts);
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

    /*
    Help setup test-case configurtation where prod/cons are located on same/diff node
    */
    void CreateProducerAndConsumerHelper(int workerNum, std::shared_ptr<Producer> &p1, std::shared_ptr<Consumer> &c1,
                                         std::shared_ptr<Consumer> &c2, std::string stream)
    {
        SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
        SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

        if (workerNum == ONE_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(stream, p1, defaultProducerConf_));
            DS_ASSERT_OK(client1_->Subscribe(stream, config1, c1));
            DS_ASSERT_OK(client1_->Subscribe(stream, config2, c2));
        } else if (workerNum == TWO_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(stream, p1, defaultProducerConf_));
            DS_ASSERT_OK(client2_->Subscribe(stream, config1, c1));
            DS_ASSERT_OK(client2_->Subscribe(stream, config2, c2));
        } else if (workerNum == THREE_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(stream, p1, defaultProducerConf_));
            DS_ASSERT_OK(client2_->Subscribe(stream, config1, c1));
            DS_ASSERT_OK(client3_->Subscribe(stream, config2, c2));
        }
    }

protected:
    void InitTest()
    {
        uint32_t workerIndex = 0;
        HostPort workerAddress1;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex++, workerAddress1));
        HostPort workerAddress2;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex++, workerAddress2));
        HostPort workerAddress3;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress3));
        InitStreamClient(0, client1_);
        InitStreamClient(1, client2_);
        InitStreamClient(2, client3_); // worker index is 2
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }
    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";

    const int ONE_WORKER = 1;
    const int TWO_WORKER = 2;
    const int THREE_WORKER = 3;

    // cluster config
    int numWorkers = 3;
    int numEtcd = 1;
    int numRpcThreads = 0;
};

/*
Create 1 producer 2 consumer. Create send thread for producer. Create receive thread for consumer2.
Consumer1 closes during data send and receive. Wait for threads. Consumer2 still receives all data
sent by producer.
*/
TEST_F(SPMCTest, CloseOneConsumerDuringSendAndReceive)
{
    for (int workers = 1; workers <= numWorkers; workers++) {
        LOG(INFO) << "Start test with configuration: " << workers;
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer1;
        std::shared_ptr<Consumer> consumer2;
        std::string streamName = "Close1ConDuringSendRecv" + std::to_string(workers);
        CreateProducerAndConsumerHelper(workers, producer, consumer1, consumer2, streamName);

        int timeOut = 10000;
        int numElement = 10;
        std::string data[numElement];
        std::vector<Element> outElements;
        LOG(INFO) << "outElements size: " << outElements.size();
        std::thread sendThread([&]() {
            for (int i = 0; i < numElement; i++) {
                data[i] = "Hello World" + std::to_string(i);
                uint8_t id = i + 1;
                Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
                element.id = id;
                DS_ASSERT_OK(producer->Send(element));
            }
        });
        std::thread receiveThread(
            [&]() { ASSERT_EQ(consumer2->Receive(numElement, timeOut, outElements), Status::OK()); });
        DS_ASSERT_OK(consumer1->Close());
        sendThread.join();
        receiveThread.join();

        ASSERT_EQ(outElements.size(), size_t(numElement));
        LOG(INFO) << "outElements size: " << outElements.size();
        LOG(INFO) << "End test with configuration: " << workers;
    }
}

/*
Create 1 producer 2 consumers. Create a send thread for producer to send. Both consumer1 and
consumer2 are closed during send. Nothing should be received and outElements still empty.
Producer sends normally.
*/
TEST_F(SPMCTest, CloseBothConsumerDuringSend)
{
    for (int workers = 1; workers <= numWorkers; workers++) {
        LOG(INFO) << "Start test with configuration: " << workers;
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer1;
        std::shared_ptr<Consumer> consumer2;
        std::string streamName = "Close2ConDuringSendRecv" + std::to_string(workers);
        CreateProducerAndConsumerHelper(workers, producer, consumer1, consumer2, streamName);

        int numElement = 10;
        std::string data[numElement];
        std::vector<Element> outElements;
        std::thread sendThread([&]() {
            for (int i = 0; i < numElement; i++) {
                data[i] = "Hello World" + std::to_string(i);
                uint8_t id = i + 1;
                Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
                element.id = id;
                DS_ASSERT_OK(producer->Send(element));
            }
        });
        DS_ASSERT_OK(consumer1->Close());
        DS_ASSERT_OK(consumer2->Close());
        sendThread.join();

        ASSERT_EQ(outElements.size(), size_t(0));
        LOG(INFO) << "End test with configuration: " << workers;
    }
}

/*
Create 1 producer 2 consumers. create 3 threads. Two for receive, one for each consumer.
One thread for producer1 send. Close producer1 during receive and send. Create new producer2
to continue sending. Both consumers should be able to receive all elements from both producers.
*/
TEST_F(SPMCTest, DISABLED_NewProducerSend)
{
    for (int workers = 1; workers <= numWorkers; workers++) {
        LOG(INFO) << "Start test with configuration: " << workers;
        std::shared_ptr<Producer> producer1;
        std::shared_ptr<Consumer> consumer1;
        std::shared_ptr<Consumer> consumer2;
        std::string streamName = "testNewProducerSend" + std::to_string(workers);
        CreateProducerAndConsumerHelper(workers, producer1, consumer1, consumer2, streamName);

        int timeOut = 10000;
        int numElement = 10;
        int totalElement = 20;
        std::string data[numElement];
        std::vector<Element> outElements1;
        std::vector<Element> outElements2;

        std::thread receiveThread1(
            [&]() { ASSERT_EQ(consumer1->Receive(totalElement, timeOut, outElements1), Status::OK()); });
        std::thread receiveThread2(
            [&]() { ASSERT_EQ(consumer2->Receive(totalElement, timeOut, outElements2), Status::OK()); });
        std::thread sendThread([&]() {
            for (int i = 0; i < numElement; i++) {
                data[i] = "Hello World" + std::to_string(i);
                uint8_t id = i + 1;
                Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
                element.id = id;
                DS_ASSERT_OK(producer1->Send(element));
            }
        });
        const int SLEEP_TIME = 10;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
        DS_ASSERT_OK(producer1->Close());
        std::shared_ptr<Producer> producer2;
        DS_ASSERT_OK(client1_->CreateProducer(streamName, producer2, defaultProducerConf_));
        for (int i = 0; i < numElement; i++) {
            data[i] = "Hello World" + std::to_string(i);
            uint8_t id = i + 1;
            Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
            element.id = id;
            DS_ASSERT_OK(producer2->Send(element));
        }
        receiveThread1.join();
        receiveThread2.join();
        sendThread.join();

        ASSERT_EQ(outElements1.size(), size_t(totalElement));
        ASSERT_EQ(outElements2.size(), size_t(totalElement));

        LOG(INFO) << "End test with configuration: " << workers;
    }
}

class StreamReserveMemoryTest : public SPMCTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = numWorkers;
        opts.numEtcd = numEtcd;
        opts.numRpcThreads = numRpcThreads;
        opts.workerGflagParams = " -sc_local_cache_memory_size_mb=2 -page_size=" + std::to_string(pageSize_);
        SCClientCommon::SetClusterSetupOptions(opts);
    }
};

TEST_F(StreamReserveMemoryTest, TestReserveLocalCacheMemory1)
{
    // Make sure that Subcribe can be rejected if memory reservation fails for local cache memory.
    // sc_local_cache_memory_size_mb is set to 2MB, so it can only accept 2 streams in this case.
    std::string streamName1 = "testReservelocalCacheMem1";
    std::string streamName2 = "testReservelocalCacheMemTwo";
    std::string streamName3 = "testReservelocalCacheMemThree";
    ProducerConf conf;
    conf.pageSize = MB;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1_->CreateProducer(streamName1, producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client1_->CreateProducer(streamName2, producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(client1_->CreateProducer(streamName3, producer3, conf));
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName1, config1, consumer1));
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName2, config2, consumer2));
    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_NOT_OK(client2_->Subscribe(streamName3, config3, consumer3));
}

TEST_F(StreamReserveMemoryTest, TestReserveLocalCacheMemory2)
{
    // Make sure that CloseConsumer would early reclaim the reservation.
    // sc_local_cache_memory_size_mb is set to 2MB, so it can only accept 2 streams with consumers in this case.
    std::string streamName1 = "testReservelocalCacheMem1";
    std::string streamName2 = "testReservelocalCacheMemTwo";
    std::string streamName3 = "testReservelocalCacheMemThree";
    ProducerConf conf;
    conf.pageSize = MB;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1_->CreateProducer(streamName1, producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client1_->CreateProducer(streamName2, producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(client1_->CreateProducer(streamName3, producer3, conf));
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName1, config1, consumer1));
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName2, config2, consumer2));
    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    // Consumer cannot be created because the 2 streams used up the local cache memory.
    DS_ASSERT_NOT_OK(client2_->Subscribe(streamName3, config3, consumer3));
    // Now after we close consumer2, local cache memory is reclaimed, so consumer3 can be created.
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(client2_->Subscribe(streamName3, config3, consumer3));
    // Now we do the opposite to make sure the reservation can still be done for stream2.
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(client2_->Subscribe(streamName2, config3, consumer2));
    DS_ASSERT_NOT_OK(client2_->Subscribe(streamName3, config3, consumer3));
}

TEST_F(ConsumerTest, TestOneMsTimeout)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("TestTimeoutMs", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("TestTimeoutMs", config, consumer));

    int dataNum = 5;
    std::vector<std::string> dataList;
    for (int i = 0; i < dataNum; ++i) {
        std::string data = "Test-Data" + std::to_string(i);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));
        dataList.emplace_back(data);
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(dataNum, 1, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(dataNum));
}

TEST_F(ConsumerTest, TestParallelConsumerUse)
{
    std::shared_ptr<Producer> producer;
    defaultProducerConf_.retainForNumConsumers = 1;
    DS_ASSERT_OK(client_->CreateProducer("testParallelConsumerUse", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("testParallelConsumerUse", config, consumer));

    std::string data = "Hello";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer->Send(element));

    DS_ASSERT_OK(datasystem::inject::Set("CheckAndSetInUse.success.sleep", "sleep(5000)"));

    // Create a consumer thread that Receive() last at least 5 seconds.
    ThreadPool pool(1);
    auto consumerReceiveFunc([&consumer]() {
        std::vector<Element> outElements;
        return consumer->Receive(1, RPC_TIMEOUT, outElements);
    });
    std::future<Status> fut = pool.Submit([&consumerReceiveFunc]() { return consumerReceiveFunc(); });

    sleep(1);

    // Parallel call from the same consumer should fail.
    StatusCode expectedCode = K_SC_STREAM_IN_USE;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, RPC_TIMEOUT, outElements).GetCode(), expectedCode);
    ASSERT_EQ(consumer->Ack(0).GetCode(), expectedCode);
    ASSERT_EQ(consumer->Close().GetCode(), expectedCode);

    DS_ASSERT_OK(fut.get());

    DS_ASSERT_OK(datasystem::inject::Clear("CheckAndSetInUse.success.sleep"));

    DS_ASSERT_OK(consumer->Receive(1, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), 1);
    DS_ASSERT_OK(consumer->Ack(outElements[0].id));
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ConsumerTest, TestRecvDelay)
{
    std::shared_ptr<Producer> producer;
    const int delayFlushTimeMs = 10;
    defaultProducerConf_.delayFlushTime = delayFlushTimeMs;
    DS_ASSERT_OK(client_->CreateProducer("stream", producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("stream", config, consumer));

    std::string data = "Hello";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    Timer timer;
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, RPC_TIMEOUT, outElements));
    double delayLimit = 500; // ms
    ASSERT_LT(timer.ElapsedMilliSecond(), delayLimit);
}
}  // namespace st
}  // namespace datasystem

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

#include <utility>

#include <gtest/gtest.h>

#include "common.h"
#include "sc_client_common.h"
#include "client/stream_cache/pub_sub_utils.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/random_data.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class ResetStreamTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = " -v=3";
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestClientInstance();
    }

    void TearDown() override
    {
        client_ = nullptr;
        client2_ = nullptr;
        client3_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTestClientInstance()
    {
        int32_t timeoutMs = 60000; // timeout is 60000 ms
        InitStreamClient(0, client_, timeoutMs, true);
        InitStreamClient(1, client2_, timeoutMs, true);
        InitStreamClient(2, client3_, timeoutMs, true);
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    Status DataFlowAfterResetResume(std::shared_ptr<Producer> producer, std::shared_ptr<Consumer> consumer)
    {
        // Check if the previous data is received or the new one.
        std::string data = "This is different data";
        std::vector<Element> outElements;
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        RETURN_IF_NOT_OK(producer->Send(element));

        RETURN_IF_NOT_OK(consumer->Receive(1, 10000, outElements));
        if (outElements.size() == 0) {
            return Status(K_RUNTIME_ERROR, "Didn't receive any element");
        }
        std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
        LOG(INFO) << "Original " << data << " actual data " << actualData;
        CHECK_FAIL_RETURN_STATUS(data == actualData, K_RUNTIME_ERROR, "received data is different");
        return Status::OK();
    }

    void SendDataContinuously(std::shared_ptr<Producer> prod1, std::shared_ptr<Producer> prod2,
                              std::shared_ptr<Producer> prod3)
    {
        std::string data = RandomData().GetRandomString(200);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        Status rc;
        while (true) {
            rc = prod1->Send(element);
            if (rc.IsOk()) {
                rc = prod2->Send(element);
            }
            if (rc.IsOk()) {
                rc = prod3->Send(element);
            }
            if (rc.IsError()) {
                EXPECT_EQ(rc.GetCode(), K_SC_STREAM_IN_RESET_STATE);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    Status MultiHopDataMovement(std::string &data, std::shared_ptr<Producer> prod1, std::shared_ptr<Producer> prod2,
                                std::shared_ptr<Consumer> con1, std::shared_ptr<Consumer> con2)
    {
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        RETURN_IF_NOT_OK(prod1->Send(element));

        std::vector<Element> outElements;
        RETURN_IF_NOT_OK(con1->Receive(1, 10000, outElements));
        CHECK_FAIL_RETURN_STATUS(outElements.size() > (size_t) 0, K_RUNTIME_ERROR, "Didn't receive any data1");
        RETURN_IF_NOT_OK(prod1->Send(element));
        std::string actualData1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
        CHECK_FAIL_RETURN_STATUS(data == actualData1, K_RUNTIME_ERROR, "received data is different");

        Element element2(reinterpret_cast<uint8_t *>(&actualData1.front()), actualData1.size());
        RETURN_IF_NOT_OK(prod2->Send(element2));

        RETURN_IF_NOT_OK(con2->Receive(1, 10000, outElements));
        CHECK_FAIL_RETURN_STATUS(outElements.size() > (size_t) 0, K_RUNTIME_ERROR, "Didn't receive any data2");
        RETURN_IF_NOT_OK(prod2->Send(element2));
        std::string actualData2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
        CHECK_FAIL_RETURN_STATUS(data == actualData2, K_RUNTIME_ERROR, "received data is different");
        return Status::OK();
    }

    Status WorkerRestartAndClientDetect(int workerIdx)
    {
        // Shutdown worker
        cluster_->QuicklyShutdownWorker(workerIdx);
        // Restart worker
        cluster_->StartNode(WORKER, workerIdx, "");
        cluster_->WaitNodeReady(WORKER, workerIdx);

        // wait for heartbeat interval so client can detect worker restarted
        std::this_thread::sleep_for(std::chrono::seconds(6));
        return Status::OK();
    }
    void CreatePubSubForClients(std::vector<std::shared_ptr<StreamClient>> &clients, int clientCount,
                                std::vector<std::shared_ptr<Producer>> &producers,
                                std::vector<std::shared_ptr<Consumer>> &consumers,
                                std::vector<std::vector<std::string>> &clientStreams);
    void CreatePubSubs(std::shared_ptr<StreamClient> client, std::vector<std::shared_ptr<Producer>> &producers,
                       std::vector<std::shared_ptr<Consumer>> &consumers, int prodStart, int prodEnd, int conStart,
                       int conEnd, std::vector<std::vector<std::string>> &clientStreams);

    std::shared_ptr<StreamClient> client_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(ResetStreamTest, CloseProducersAndConsumers)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testCloseProdsAndCons";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));
    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());

    producer.reset();
    consumer.reset();
}

TEST_F(ResetStreamTest, ResetSingleStreamsLocalSubs)
{
    std::shared_ptr<Producer> producer1;
    std::string streamName1 = "testResetStreamLocalSub";
    DS_ASSERT_OK(client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName1, config1, consumer1));

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName1);
}

TEST_F(ResetStreamTest, ResetSingleStreamsRemoteSubs)
{
    std::shared_ptr<Producer> producer1;
    std::string streamName1 = "testResetStreamRemotelSub";
    DS_ASSERT_OK(client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName1, config1, consumer1));

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName1);
}

TEST_F(ResetStreamTest, ResetMultiStreamsSingleReset)
{
    std::shared_ptr<Producer> producer1;
    std::string streamName1 = "testMultiStreamSingleReset";
    DS_ASSERT_OK(client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName1, config1, consumer1));
    std::shared_ptr<Producer> producer2;
    std::string streamName2 = "test2";
    DS_ASSERT_OK(client_->CreateProducer(streamName2, producer2, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName2, config2, consumer2));

    std::shared_ptr<Producer> producer3;
    std::string streamName3 = "test3";
    DS_ASSERT_OK(client_->CreateProducer(streamName3, producer3, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName3, config3, consumer3));

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName1);
    streamNames.push_back(streamName2);
    streamNames.push_back(streamName3);
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(client_->DeleteStream(streamName1));
}

TEST_F(ResetStreamTest, ResetMultiStreamsMultiResets)
{
    std::shared_ptr<Producer> producer1;
    std::string streamName1 = "testMultiStreamMultiReset";
    DS_ASSERT_OK(client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName1, config1, consumer1));
    std::shared_ptr<Producer> producer2;
    std::string streamName2 = "test2";
    DS_ASSERT_OK(client_->CreateProducer(streamName2, producer2, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName2, config2, consumer2));

    std::shared_ptr<Producer> producer3;
    std::string streamName3 = "test3";
    DS_ASSERT_OK(client_->CreateProducer(streamName3, producer3, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName3, config3, consumer3));

    std::vector<std::string> streamNames1;
    streamNames1.push_back(streamName1);
    streamNames1.push_back(streamName2);
    std::vector<std::string> streamNames2;
    streamNames2.push_back(streamName3);

    streamNames1.push_back(streamName3);
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(client_->DeleteStream(streamName1));
}

TEST_F(ResetStreamTest, ResetDeleteStream)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testResetDelStream";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);
}

TEST_F(ResetStreamTest, ResetStreamsCrossDependencyTest1)
{
    std::shared_ptr<Producer> producer1, producer2;
    std::string streamName1 = "testResetCrossDependency_s1";
    std::string streamName2 = "testResetCrossDependency_s2";
    DS_ASSERT_OK(client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1, consumer2;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName2, config1, consumer1));

    std::shared_ptr<StreamClient> client1;
    InitStreamClient(0, client1);
    DS_ASSERT_OK(client1->CreateProducer(streamName2, producer2, defaultProducerConf_));
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName1, config2, consumer2));

    std::vector<std::string> streamNames1, streamNames2;
    streamNames1.push_back(streamName1);
    streamNames1.push_back(streamName2);
    streamNames2.push_back(streamName2);
    streamNames2.push_back(streamName1);
}

void ResetStreamTest::CreatePubSubs(
    std::shared_ptr<StreamClient> client, std::vector<std::shared_ptr<Producer>> &producers,
    std::vector<std::shared_ptr<Consumer>> &consumers, int prodStart, int prodEnd, int conStart,
    int conEnd, std::vector<std::vector<std::string>> &clientStreams)
{
    std::string streamPrefix = "ResetStreamsCrossDependencyTest2";
    std::vector<std::string> clientStream;
    if (prodEnd > 0) {
        for (int i = prodStart; i <= prodEnd; i++) {
            std::string streamName = streamPrefix + std::to_string(i);
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client->CreateProducer(streamName, producer, defaultProducerConf_));
            producers.push_back(producer);
            clientStream.push_back(streamName);
        }
    }
    if (conEnd > 0) {
        for (int i = conStart; i <= conEnd; i++) {
            std::string streamName = streamPrefix + std::to_string(i);
            std::shared_ptr<Consumer> consumer;
            std::string subName = "sub" +std::to_string(clientStreams.size()) + "_" + std::to_string(i);
            SubscriptionConfig config(subName, SubscriptionType::STREAM);
            DS_ASSERT_OK(client->Subscribe(streamName, config, consumer));
            consumers.push_back(consumer);
            clientStream.push_back(streamName);
        }
    }
    clientStreams.push_back(clientStream);
}

void ResetStreamTest::CreatePubSubForClients(std::vector<std::shared_ptr<StreamClient>> &clients, int clientCount,
                                             std::vector<std::shared_ptr<Producer>> &producers,
                                             std::vector<std::shared_ptr<Consumer>> &consumers,
                                             std::vector<std::vector<std::string>> &clientStreams)
{
    // have three blocks of streams. 1-16, 17-20 and 21-21.
    const int b1Range = 16;
    const int b2Range = 20;
    const int b3Range = 21;
    int clientIdx = 0;

    // Five client connected to worker 1 and one client connected to worker 2
    for (int i = 1; i < clientCount ; i++) {
        std::shared_ptr<StreamClient> client;
        InitStreamClient(0, client);
        clients.push_back(client);
    }

    std::shared_ptr<StreamClient> client;
    InitStreamClient(1, client);
    clients.push_back(client);
    client->Init(false);
    
    // Create pubs/subs for clients in the specific stream blocks
    CreatePubSubs(clients[clientIdx++], producers, consumers, 1, b1Range, b1Range + 1, b2Range, clientStreams);
    CreatePubSubs(clients[clientIdx++], producers, consumers, -1, -1, 1, b1Range, clientStreams);
    CreatePubSubs(clients[clientIdx++], producers, consumers, 1, b1Range, b1Range + 1, b2Range, clientStreams);
    CreatePubSubs(clients[clientIdx++], producers, consumers, b2Range + 1, b3Range, -1, -1, clientStreams);
    CreatePubSubs(clients[clientIdx++], producers, consumers, -1, -1, 1, b1Range, clientStreams);
    CreatePubSubs(clients[clientIdx++], producers, consumers, b1Range + 1, b2Range, b2Range + 1, b3Range,
                  clientStreams);
}

TEST_F(ResetStreamTest, LEVEL1_TestClientLostWorkerSameNode)
{
    std::shared_ptr<Producer> prod1;
    std::string streamName = "workerlosttestsamenode";
    DS_ASSERT_OK(client_->CreateProducer(streamName, prod1, defaultProducerConf_));

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    DS_ASSERT_OK(WorkerRestartAndClientDetect(0));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    Status rc = prod1->Send(element);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_SC_WORKER_WAS_LOST);
    // close client and producer
    client_.reset();
    prod1.reset();

    InitStreamClient(0, client_);
    DS_ASSERT_OK(client_->CreateProducer(streamName, prod1, defaultProducerConf_));

    DS_ASSERT_OK(prod1->Send(element));
}

TEST_F(ResetStreamTest, LEVEL1_TestClientLostWorkerCrossNode)
{
    std::shared_ptr<Producer> prod1;
    std::string streamName = "workerlosttestcrossnode";
    DS_ASSERT_OK(client_->CreateProducer(streamName, prod1, defaultProducerConf_));

    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName, config, con1));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(prod1->Send(element));

    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    DS_ASSERT_OK(WorkerRestartAndClientDetect(1));
    DS_ASSERT_OK(prod1->Send(element));

    std::vector<Element> outElements;
    Status rc = con1->Receive(1, 1000, outElements);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_SC_WORKER_WAS_LOST);
    // close client2 and consumer
    client2_.reset();
    con1.reset();
    InitStreamClient(1, client2_);
    DS_ASSERT_OK(client2_->Subscribe(streamName, config, con1));
}

TEST_F(ResetStreamTest, LEVEL1_TestClientNotTrackLostWorker)
{
    // Set up two clients to not track worker lost between heartbeats. One explicit and one default.
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    std::shared_ptr<StreamClient> client1, client2;
    InitStreamClient(0, client1);
    InitStreamClient(0, client2);
   
    std::shared_ptr<Producer> prod1, prod2;
    std::string streamName = "workerlosttestnottracked";
    DS_ASSERT_OK(client1->CreateProducer(streamName, prod1, defaultProducerConf_));

    std::shared_ptr<Consumer> con1, con2;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, con1));

    DS_ASSERT_OK(WorkerRestartAndClientDetect(0));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    // After restart, producer Id is erased at the worker and the existing producer can't send.
    DS_ASSERT_NOT_OK(prod1->Send(element));

    std::vector<Element> outElements;
    Status rc = con1->Receive(1, 1000, outElements);
    // After restart, consumer is lost at the worker. However, the error code is not K_SC_WORKER_WAS_LOST.
    DS_ASSERT_NOT_OK(rc);
    ASSERT_NE(rc.GetCode(), StatusCode::K_SC_WORKER_WAS_LOST);

    // However, we can add new producers and consumers as the worker lost is not tracked.
    DS_ASSERT_OK(client1->CreateProducer(streamName, prod2, defaultProducerConf_));
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config2, con2));

    DS_ASSERT_OK(DataFlowAfterResetResume(prod2, con2));
}
}  // namespace st
}  // namespace datasystem

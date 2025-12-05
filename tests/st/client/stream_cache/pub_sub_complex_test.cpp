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
#include <math.h>

#include <gtest/gtest.h>
#include <cstddef>

#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/utils/status.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/thread_pool.h"
#include "common/stream_cache/stream_common.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class PubSubComplexTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -page_size=" + std::to_string(PAGE_SIZE);
        opts.vLogLevel = 2;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        client0_ = nullptr;
        client1_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTest()
    {
        HostPort workerAddress0;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress0));
        HostPort workerAddress1;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress1));
        LOG(INFO) << FormatString("\n Worker1: <%s>\n Worker2: <%s>\n", workerAddress0.ToString(),
                                  workerAddress1.ToString());
        
        InitStreamClient(0, client0_);
        InitStreamClient(1, client1_);
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }
    std::shared_ptr<StreamClient> client0_ = nullptr;
    std::shared_ptr<StreamClient> client1_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(PubSubComplexTest, MSBiDirectionRpc)
{
    const int streamNum = 16;
    ThreadPool pool(streamNum);
    for (int i = 0; i < streamNum; ++i) {
        pool.Submit([this, i]() {
            std::string streamName = "MSBiDirectionRpc" + std::to_string(i);
            SubscriptionConfig config("sub_core", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> n1c0;
            DS_ASSERT_OK(client1_->Subscribe(streamName, config, n1c0));

            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client0_->CreateProducer(streamName, producer, defaultProducerConf_));
            DS_ASSERT_OK(producer->Close());
            DS_ASSERT_OK(n1c0->Close());
            DS_ASSERT_OK(client0_->DeleteStream(streamName));
        });
    }
}

TEST_F(PubSubComplexTest, DISABLED_TestProducerConsumerTiming)
{
    // This is a general test of ordering events.
    std::shared_ptr<Producer> prod1, prod2, prod3, prod4;
    std::shared_ptr<Consumer> localCon, remoteCon;
    DS_ASSERT_OK(client0_->CreateProducer("test_stream", prod1, defaultProducerConf_));
    DS_ASSERT_OK(client0_->CreateProducer("test_stream", prod2, defaultProducerConf_));
    DS_ASSERT_OK(client0_->CreateProducer("test_stream", prod3, defaultProducerConf_));
    DS_ASSERT_OK(client0_->CreateProducer("test_stream", prod4, defaultProducerConf_));

    SubscriptionConfig config1("localSub", SubscriptionType::STREAM);
    SubscriptionConfig config2("remoteSub", SubscriptionType::STREAM);
    // start remote consumer immediately
    DS_ASSERT_OK(client1_->Subscribe("test_stream", config2, remoteCon));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // send by all four producers
    DS_ASSERT_OK(prod1->Send(element));

    DS_ASSERT_OK(prod2->Send(element));

    // start local consumer after two flush.
    // No rows have been ack back. So its starting point is the same
    // as the remote consumer.
    DS_ASSERT_OK(client0_->Subscribe("test_stream", config1, localCon));

    DS_ASSERT_OK(prod3->Send(element));

    DS_ASSERT_OK(prod4->Send(element));

    std::vector<Element> outElements;
    DS_ASSERT_OK(remoteCon->Receive(4, -1, outElements));
    ASSERT_EQ(outElements.size(), (uint32_t) 4);

    DS_ASSERT_OK(localCon->Receive(0, outElements));
    ASSERT_EQ(outElements.size(), (uint32_t) 4);
}

TEST_F(PubSubComplexTest, DISABLED_TestProducerOrderInFlush)
{
    // Since we have no local consumer, Ack should not be a part of Flush operation.
    // Therefore, No error should appear from order of Ack inside Flush.
    // Create 5 concurrent producers in a stream, they will flush in different oreder.
    int producerNum = 5;
    ThreadPool pool(producerNum);
    for (int i = 0; i < producerNum; i++) {
        pool.Submit([this, i]() {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client0_->CreateProducer("test_stream", producer, defaultProducerConf_));

            std::string data = "Hello World " + std::to_string(i);
            Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
            if (i % 2 == 0) {
                DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.stream.sleep_while_flush", "sleep(500)"));
            } else {
                DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.stream.sleep_while_flush", "sleep(100)"));
            }
            DS_ASSERT_OK(producer->Send(element));
        });
    }
}

TEST_F(PubSubComplexTest, CreateAndCloseProducerWithinSameStream)
{
    const size_t testNum = 5;
    std::vector<std::shared_ptr<Producer>> producerList(testNum);
    for (auto &producer : producerList) {
        DS_ASSERT_OK(client0_->CreateProducer("CreateAndClose", producer, defaultProducerConf_));
    }
    for (auto &producer : producerList) {
        DS_ASSERT_OK(producer->Close());
    }
}

TEST_F(PubSubComplexTest, CreateAndCloseProducerWithinDiffStream)
{
    const size_t testNum = 20;
    const size_t producerOneStream = 5;
    std::vector<std::shared_ptr<Producer>> producerList(testNum);
    size_t cnt = 0;
    std::string streamName;
    for (auto &producer : producerList) {
        streamName = "test" + std::to_string(cnt / producerOneStream);
        DS_ASSERT_OK(client0_->CreateProducer(streamName, producer, defaultProducerConf_));
        cnt++;
    }
    for (auto &producer : producerList) {
        DS_ASSERT_OK(producer->Close());
    }
}

TEST_F(PubSubComplexTest, SubscribeInStreamMode)
{
    const size_t testNum = 5;
    const uint64_t mockId = 0;
    std::vector<std::shared_ptr<Consumer>> consumerList(testNum);
    for (size_t i = 0; i < testNum; ++i) {
        std::string subName = "sub" + std::to_string(i);
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        DS_ASSERT_OK(client0_->Subscribe("testSubscribeInStreamMode", config, consumerList[i]));
    }
    for (size_t i = 0; i < testNum; ++i) {
        DS_ASSERT_OK(consumerList[i]->Close());
        DS_EXPECT_NOT_OK(consumerList[i]->Ack(mockId));
    }
}

TEST_F(PubSubComplexTest, SubscribeDuplicateStreamMode)
{
    const size_t testNum = 2;
    std::vector<std::shared_ptr<Consumer>> consumerList(testNum);

    std::string subName = "sub1";
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    DS_EXPECT_OK(client0_->Subscribe("DuplicateSub", config, consumerList[0]));
    DS_EXPECT_NOT_OK(client0_->Subscribe("DuplicateSub", config, consumerList[1]));
    DS_EXPECT_OK(consumerList[0]->Close());
}

TEST_F(PubSubComplexTest, MultiStreamInMixMode)
{
    // Create 2 Streams and 4 Producers, each Stream has 2 Producers
    const size_t streamNum = 2;
    std::vector<std::string> streamNameList;
    std::vector<std::shared_ptr<Producer>> producerList(streamNum * 2);

    for (size_t i = 0; i < streamNum; ++i) {
        streamNameList.emplace_back("MultiStreamInMixMode" + std::to_string(i));
        DS_EXPECT_OK(client0_->CreateProducer(streamNameList.back(), producerList[2 * i], defaultProducerConf_));
        DS_EXPECT_OK(client0_->CreateProducer(streamNameList.back(), producerList[2 * i + 1], defaultProducerConf_));
    }

    // Now we create 5 Consumers, they will have relationship with 2 stream
    std::vector<SubscriptionConfig> configList;

    // First 2 Consumers sub with Stream <stream0> and <stream1> in STREAM mode
    configList.emplace_back(SubscriptionConfig("sub0", SubscriptionType::ROUND_ROBIN));
    configList.emplace_back(SubscriptionConfig("sub1", SubscriptionType::STREAM));

    std::vector<std::shared_ptr<Consumer>> consumerList(streamNum);
    for (size_t i = 0; i < consumerList.size(); ++i) {
        DS_ASSERT_OK(client0_->Subscribe(streamNameList[i], configList[1], consumerList[i]));
    }
    // The third one sub with Stream <stream0> Subscription <sub1> in STREAM mode, hence fail
    std::shared_ptr<Consumer> dupStreamConsumer;
    DS_ASSERT_NOT_OK(client0_->Subscribe(streamNameList[0], configList[1], dupStreamConsumer));

    // The fourth one sub with Stream <stream1> in QUEUE mode, hence fail
    std::shared_ptr<Consumer> queueConsumer;
    DS_ASSERT_NOT_OK(client0_->Subscribe(streamNameList[1], configList[0], queueConsumer));

    for (auto &producer : producerList) {
        DS_EXPECT_OK(producer->Close());
        // Second close Should be no-op if close one Producer twice
        DS_EXPECT_OK(producer->Close());
    }
    for (auto &consumer : consumerList) {
        DS_ASSERT_OK(consumer->Close());
        // Second close should be no-op if close one Consumer twice
        DS_ASSERT_OK(consumer->Close());
    }
}

TEST_F(PubSubComplexTest, TestSyncSubTableRetry)
{
    // Maintain a connection from worker0 to worker1 by creating a dummy stream s2.
    std::string s2("dummyStream");
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1_->Subscribe(s2, config2, consumer2));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client0_->CreateProducer(s2, producer2, defaultProducerConf_));

    // Create a stream s1 from worker0 to worker1
    std::string s1("SyncSubTableRetry");
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1_->Subscribe(s1, config1, consumer1));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client0_->CreateProducer(s1, producer1, defaultProducerConf_));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "RemoteWorker.SleepBeforeAsyncRead", "call(500)"));
    const size_t numElements = 1000;
    const size_t elementLength = 1024;
    std::string data(elementLength, 'a');
    Element ele(reinterpret_cast<uint8_t *>(data.data()), data.size());
    for (size_t i = 0; i < numElements; ++i) {
        DS_ASSERT_OK(producer1->Send(ele));
    }
    producer1->Close();
    consumer1->Close();
    client1_->DeleteStream(s1);

    // Start all over again. Same stream name
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "RemoteWorker.SleepBeforeAsyncRead"));
    DS_ASSERT_OK(client1_->Subscribe(s1, config1, consumer1));
    DS_ASSERT_OK(client0_->CreateProducer(s1, producer1, defaultProducerConf_));
    for (size_t i = 0; i < numElements; ++i) {
        DS_ASSERT_OK(producer1->Send(ele));
    }
    std::vector<Element> out;
    out.reserve(numElements);
    const uint64_t timeoutMs = 60'000;
    DS_ASSERT_OK(consumer1->Receive(numElements, timeoutMs, out));
    ASSERT_EQ(out.size(), numElements);
    producer1->Close();
    consumer1->Close();
    client1_->DeleteStream(s1);
}

TEST_F(PubSubComplexTest, TestCreateWritePageTimeout)
{
    auto func = [this](int i) -> Status {
        defaultProducerConf_.pageSize = 4 * 1024;
        std::string streamName = "stream" + std::to_string(i);
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(client1_->Subscribe(streamName, config, consumer));
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(client0_->CreateProducer(streamName, producer, defaultProducerConf_));

        const size_t numElements = 100;
        const uint64_t timeoutMs = 10'000;
        const size_t minEleSize = 1024;
        const size_t maxEleSize = 5 * 1024;
        int residualReqTimeoutDurationTimeoutMs = 10;
        RETURN_IF_NOT_OK(cluster_->SetInjectAction(WORKER, 0, "ZmqService::RouteToRegBackend",
                                                   FormatString("call(%d)", residualReqTimeoutDurationTimeoutMs)));
        for (size_t i = 0; i < numElements; ++i) {
            const size_t elementSize = randomData_.GetRandomUint64(minEleSize, maxEleSize);
            std::string data(elementSize, 'a');
            Element ele(reinterpret_cast<uint8_t *>(data.data()), data.size());
            reqTimeoutDuration.Init(residualReqTimeoutDurationTimeoutMs);
            RETURN_IF_NOT_OK(producer->Send(ele));
        }
        std::vector<Element> out;
        out.reserve(numElements);
        RETURN_IF_NOT_OK(consumer->Receive(numElements, timeoutMs, out));
        CHECK_FAIL_RETURN_STATUS(out.size() == numElements, K_RUNTIME_ERROR, "");
        return Status::OK();
    };
    DS_ASSERT_OK(func(0));
}

}  // namespace st
}  // namespace datasystem

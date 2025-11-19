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
#include "sc_client_common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
const uint32_t EXPECT_RECV_NUM = 10;
const uint32_t TEST_PAGE_SIZE = 20 * 1024;
constexpr int K_TWO = 2;
constexpr int K_FOUR = 4;
constexpr int K_TEN = 10;
class StreamDfxSendRecvTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int vLogLevel = 3;
        const uint32_t workerCout = 2;
        opts.numEtcd = 1;
        opts.numWorkers = workerCout;
        opts.vLogLevel = vLogLevel;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(InitClient(0, client1_));
        DS_ASSERT_OK(InitClient(1, client2_));
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
        options.accessKey = accessKey_;
        options.secretKey = secretKey_;
        options.host = workerAddress.Host();
        options.port = workerAddress.Port();
        client = std::make_shared<StreamClient>(options);
        return client->Init();
    }

    Status CreateConsumer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          const std::string &subName, std::shared_ptr<Consumer> &consumer)
    {
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        return client->Subscribe(streamName, config, consumer);
    }

    Status CreateProducer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          std::shared_ptr<Producer> &producer)
    {
        const int64_t autoFlushTime = 10 * 1000;  // 10s;
        ProducerConf conf = { .delayFlushTime = autoFlushTime,
                              .pageSize = TEST_PAGE_SIZE,
                              .maxStreamSize = TEST_STREAM_SIZE };
        return client->CreateProducer(streamName, producer, conf);
    }

    Status TryAndDeleteStream(std::shared_ptr<StreamClient> &spClient, std::string streamName)
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

    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamDfxSendRecvTest, TestRecv)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;
    std::shared_ptr<Consumer> consumer2;
    std::string streamName = "test_stream_recv";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname1", consumer1));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname2", consumer2));

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(EXPECT_RECV_NUM, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 2ul);
    DS_ASSERT_OK(consumer1->Ack(outElements.back().id));

    ASSERT_EQ(consumer2->Receive(EXPECT_RECV_NUM, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 2ul);
    DS_ASSERT_OK(consumer2->Ack(outElements.back().id));
    DS_ASSERT_OK(consumer2->Ack(outElements.back().id));
}

TEST_F(StreamDfxSendRecvTest, TestRecvWorkerCrash)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;
    std::shared_ptr<Consumer> consumer2;

    // Create a producer on node1
    std::string streamName = "RecvWorkerCrash";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));

    // Create a consumer on node2
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname1", consumer1));
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname2", consumer2));

    // Delay the recv on node2
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "PushElementsCursors.begin",
                                           "sleep(3000)"));

    // Send a lot of data in producer
    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());
    for (int i = 0; i < 10; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(10, 5000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 10);
    DS_ASSERT_OK(consumer1->Ack(10));

    // Restart node2
    // Node with consumers - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
}

TEST_F(StreamDfxSendRecvTest, TestPendingRecvWorkerCrash)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;

    // Create a producer on node1
    std::string streamName = "test_stream_pendingRecvWorkerCrash";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));

    // Create a consumer on node2
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname1", consumer1));

    // Send a lot of data in producer
    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());
    for (int i = 0; i < 100; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    int threadNum = 1;
    ThreadPool pool(threadNum);
    auto fut1 = pool.Submit([this, consumer1, element]() {
        // Make sure receive is still blocked while we shutdown the node
        std::vector<Element> outElements;
        ASSERT_EQ(consumer1->Receive(1000, 50000, outElements).GetCode(), StatusCode::K_SC_ALREADY_CLOSED);
        ASSERT_EQ(outElements.size(), 0);
    });

    // Restart node2
    // Node with consumers - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    // Wait for recieve
    fut1.get();
}

TEST_F(StreamDfxSendRecvTest, LEVEL1_TestWorkerCrashLateEtcdNotification)
{
    LOG(INFO) << "TestWorkerCrashLateEtcdNotification start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    const int DEFAULT_NUM_ELEMENT = 100;
    const int DEFAULT_WAIT_TIME = 5000;

    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(K_FOUR * KB);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ProducerConf conf;
    conf.maxStreamSize = K_TEN * MB;
    conf.pageSize = 1 * MB;

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamMetadata.ClearPubSubMetaData.sleep", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "StreamMetadata.ClearPubSubMetaData.sleep", "sleep(5000)"));

    // Create a new producer - this data should be retain
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    // Node with consumers - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config1, consumer1));

    // Try sending and getting data
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer1->Send(element));
    }

    // Try to receive data
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer1->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), DEFAULT_NUM_ELEMENT);
    DS_ASSERT_OK(consumer1->Ack(DEFAULT_NUM_ELEMENT));
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer->Close());
    outElements.clear();
    DS_ASSERT_OK(TryAndDeleteStream(client1, streamName));
}

TEST_F(StreamDfxSendRecvTest, LEVEL1_TestResendDataAfterProducerWorkerCrash)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;

    // Create a producer on node1
    std::string streamName = "test_stream_resendAfterProdWorkerCrash";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));

    // Create a consumer on node2
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname1", consumer1));

    // Send a lot of data in producer
    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());
    for (int i = 0; i < 100; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Let recv worker get some data
    std::vector<Element> outElements;
    const int numElements = 100;
    ASSERT_EQ(consumer1->Receive(numElements, 5000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), numElements);
    DS_ASSERT_OK(consumer1->Ack(numElements));

    // Restart node1 - producer worker
    // Node with producer - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    // Create a producer on node1 again for same stream
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer1));

    // Send and recv first batch
    for (int i = 0; i < 100; i++) {
        DS_ASSERT_OK(producer1->Send(element));
    }

    // We should be able to get this data
    outElements.clear();
    ASSERT_EQ(consumer1->Receive(numElements, 5000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), numElements);
    DS_ASSERT_OK(consumer1->Ack(numElements));
}

TEST_F(StreamDfxSendRecvTest, LEVEL1_TestResendDataAfterConsumerWorkerCrash)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;

    // Create a producer on node1
    std::string streamName = "test_stream_resendAfterConWorkerCrash";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));

    // Create a consumer on node2
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname1", consumer1));

    // Send a lot of data in producer
    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());
    for (int i = 0; i < 100; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // Let recv worker get some data
    const int numElements = 100;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(numElements, 5000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), numElements);
    DS_ASSERT_OK(consumer1->Ack(numElements));

    // Restart node1 - producer worker
    // Node with producer - crash happens
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    // Create a producer on node1 again for same stream
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname1", consumer2));

    for (int i = 0; i < 100; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }

    // We should be able to get this data
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(numElements, 5000, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), numElements);
    DS_ASSERT_OK(consumer2->Ack(numElements));
}
}  // namespace st
}  // namespace datasystem

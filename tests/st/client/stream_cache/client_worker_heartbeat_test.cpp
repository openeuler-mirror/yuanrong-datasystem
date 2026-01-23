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
 * Description:
 */
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/common/inject/inject_point.h"

using namespace datasystem::client::stream_cache;

namespace datasystem {
namespace st {
class ClientWorkerSCHeartbeatTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.masterIdx = 1;
        opts.numEtcd = 1;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

class ClientSC {
public:
    explicit ClientSC(std::string streamName) : streamName_(std::move(streamName))
    {
    }
    ~ClientSC() = default;

    Status InitTestClient(const std::string &ip, const int &port, int timeout = 60000);

    Status CreateProducer(std::shared_ptr<Producer> &producer);

    Status Subscribe(const std::string &subName, std::shared_ptr<Consumer> &consumer);

    Status QueryLocalProducerNum(uint64_t &localProducerNum);

    Status QueryLocalConsumerNum(uint64_t &localProducerNum);

private:
    std::string streamName_;
    std::unique_ptr<StreamClient> client_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

Status ClientSC::InitTestClient(const std::string &ip, const int &port, int timeout)
{
    ConnectOptions connectOptions;
    connectOptions.host = ip;
    connectOptions.port = port;
    connectOptions.connectTimeoutMs = timeout;
    connectOptions.SetAkSkAuth(accessKey_, secretKey_, "");
    client_ = std::make_unique<StreamClient>(connectOptions);
    return client_->Init();
}

Status ClientSC::CreateProducer(std::shared_ptr<Producer> &producer)
{
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    return client_->CreateProducer(streamName_, producer, conf);
}

Status ClientSC::Subscribe(const std::string &subName, std::shared_ptr<Consumer> &consumer)
{
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    return client_->Subscribe(streamName_, config, consumer);
}

Status ClientSC::QueryLocalProducerNum(uint64_t &localProducerNum)
{
    auto rc = client_->QueryGlobalProducersNum(streamName_, localProducerNum);
    return rc;
}

Status ClientSC::QueryLocalConsumerNum(uint64_t &localConsumerNum)
{
    auto rc = client_->QueryGlobalConsumersNum(streamName_, localConsumerNum);
    return rc;
}

TEST_F(ClientWorkerSCHeartbeatTest, TestOneClientCrash)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    LOG(INFO) << "start create client 1";
    auto client1 = std::make_unique<ClientSC>("OneClientCrash1");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(producer1));
    std::shared_ptr<Consumer> consumer1;
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client1->Subscribe("sub1", consumer1));
    DS_ASSERT_OK(client1->Subscribe("sub2", consumer2));

    uint64_t queryRet = 0;
    DS_ASSERT_OK(client1->QueryLocalProducerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(1));
    queryRet = 0;
    DS_ASSERT_OK(client1->QueryLocalConsumerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(2));
    producer1.reset();
    consumer1.reset();
    consumer2.reset();
    client1.reset();

    // sleep 1s
    usleep(1'000'000);
    LOG(INFO) << "start create client 2";
    auto client2 = std::make_unique<ClientSC>("OneClientCrash2");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(client2->Subscribe("sub3", consumer3));
    queryRet = 0;
    DS_ASSERT_OK(client2->QueryLocalProducerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(1));
    queryRet = 0;
    DS_ASSERT_OK(client2->QueryLocalConsumerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(1));
    producer2->Close();
    consumer3->Close();
}

TEST_F(ClientWorkerSCHeartbeatTest, TestWorkerCrashAndClientCanIdentify)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    const int timeout = 2000;
    auto client = std::make_unique<ClientSC>("ClientCanIdentify");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port(), timeout));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer(producer));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe("subName", consumer));

    // shutdown worker
    LOG(INFO) << "shutdown worker" << WORKER;
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    
    // Since the worker already exited, can allow client to exit quickly
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.Disconnect.ShutdownQuickily", "call(200)"));
    DS_ASSERT_NOT_OK(producer->Close());
}

TEST_F(ClientWorkerSCHeartbeatTest, TestNormalWorkerCrash)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    LOG(INFO) << "start create client";
    auto client1 = std::make_unique<ClientSC>("NormalWorkerCrash1");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(producer1));

    // Shutdown worker
    cluster_->ShutdownNode(WORKER, 0);
    // Client enable test
    uint64_t queryRet = 0;
    auto rc = client1->QueryLocalProducerNum(queryRet);
    ASSERT_EQ(rc.IsError(), true);
    // Restart worker
    cluster_->StartNode(WORKER, 0, "");
    cluster_->WaitNodeReady(WORKER, 0);

    // Old Producer enable test
    // New producer and consumer test
    auto client2 = std::make_unique<ClientSC>("NormalWorkerCrash2");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client2->Subscribe("sub2", consumer));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size(), ULONG_MAX);
    DS_ASSERT_OK(producer2->Send(element));

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);
}

TEST_F(ClientWorkerSCHeartbeatTest, TestWorkerCrashAndClientReadEleNormally)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    const int timeout = 2000;
    auto client = std::make_unique<ClientSC>("ClientReadNormally");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port(), timeout));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer(producer));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe("subName", consumer));

    std::string data = "abc";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size(), ULONG_MAX);
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 10000, outElements), Status::OK());

    // shutdown worker
    LOG(INFO) << "shutdown worker" << WORKER;
    cluster_->QuicklyShutdownWorker(0);

    // Since the worker already exited, can allow client to exit quickly
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.Disconnect.ShutdownQuickily", "call(200)"));
    LOG(INFO) << "After closing the worker, read element again";
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    ASSERT_EQ(data, actualData);
}

TEST_F(ClientWorkerSCHeartbeatTest, LEVEL1_TestSignalTermWorker)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    auto client1 = std::make_unique<ClientSC>("testSignalTermWorker");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));

    pid_t pid = cluster_->GetWorkerPid(0);
    ASSERT_NE(pid, -1);
    cluster_->ShutdownNodes(WORKER);

    uint64_t queryRet = 0;
    auto rc = client1->QueryLocalProducerNum(queryRet);
    ASSERT_EQ(rc.IsError(), true);
}

TEST_F(ClientWorkerSCHeartbeatTest, TestOneClientCrashAndReceive)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    LOG(INFO) << "start create client 1";
    auto client1 = std::make_unique<ClientSC>("testOneClientCrashAndRecv");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(producer1));
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(client1->Subscribe("sub1", consumer1));

    LOG(INFO) << "start create client 2";
    auto client2 = std::make_unique<ClientSC>("testOneClientCrashAndRecv");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub2", consumer2));

    uint64_t queryRet = 0;
    DS_ASSERT_OK(client1->QueryLocalProducerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(1));
    queryRet = 0;
    DS_ASSERT_OK(client1->QueryLocalConsumerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(2));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size(), ULONG_MAX);
    DS_ASSERT_OK(producer1->Send(element));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer1->Receive(1, 0, outElements), Status::OK());
    std::string actualData1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData1);
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(1, 0, outElements), Status::OK());
    std::string actualData2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData2);

    consumer2.reset();
    client2.reset();
    outElements.clear();

    DS_ASSERT_OK(producer1->Send(element));
    ASSERT_EQ(consumer1->Receive(1, 0, outElements), Status::OK());
    std::string actualData3(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData3);
}

TEST_F(ClientWorkerSCHeartbeatTest, TestOneClientCrashWhenOtherClientCreate)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    std::string streamName = "testOneClientCrashWhenOtherCreate";
    std::thread clientCrash([&workerAddress, streamName]() {
        int crashTimes = 2;
        while (crashTimes > 0) {
            auto client1 = std::make_unique<ClientSC>(streamName);
            DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
            std::shared_ptr<Consumer> consumer1;
            DS_ASSERT_OK(client1->Subscribe("threadSub", consumer1));
            client1.reset();
            crashTimes--;
        }
    });

    auto client2 = std::make_unique<ClientSC>(streamName);
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::string> subNameList{ "mainSub1", "mainSub2", "mainSub3", "mainSub4", "mainSub5" };
    for (int i = 0; i < 5; i++) {
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client2->Subscribe(subNameList[i], consumer));
        consumers.emplace_back(consumer);
    }
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client2->CreateProducer(producer));
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size(), ULONG_MAX);
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    for (int i = 0; i < 5; i++) {
        outElements.clear();
        ASSERT_EQ(consumers[i]->Receive(1, 0, outElements), Status::OK());
        std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
        EXPECT_EQ(data, actualData);
    }

    clientCrash.join();
    uint64_t queryRet = 0;
    DS_ASSERT_OK(client2->QueryLocalProducerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(1));
    queryRet = 0;
    DS_ASSERT_OK(client2->QueryLocalConsumerNum(queryRet));
    ASSERT_EQ(queryRet, size_t(5));
}

TEST_F(ClientWorkerSCHeartbeatTest, TestOneClientCrashWhenOtherProducerFlush)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    std::string streamName = "testOneClientCrashWhenOtherProdFlush";
    int sendCount = 30;
    ThreadPool pool(4);
    pool.Submit([&workerAddress, sendCount, streamName]() {
        auto client = std::make_unique<ClientSC>(streamName);
        DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client->Subscribe("mainSub", consumer));
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(producer));
        std::this_thread::sleep_for(std::chrono::milliseconds(9));
        std::string data[30];
        for (int i = 0; i < sendCount; i++) {
            data[i] = "Hello World" + std::to_string(i);
            uint64_t id = i + 1;
            Element element(reinterpret_cast<uint8_t *>(&data[i].front()), data[i].size());
            element.id = id;
            DS_ASSERT_OK(producer->Send(element));
        }
        std::vector<Element> outElements;
        ASSERT_EQ(consumer->Receive(sendCount, 1000, outElements), Status::OK());
        ASSERT_EQ(outElements.size(), size_t(30));
        for (int i = 0; i < sendCount; i++) {
            ASSERT_EQ(outElements[i].id, static_cast<size_t>(i + 1));
            std::string actualData(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
            ASSERT_EQ(data[i], actualData);
        }
    });
    pool.Submit([&workerAddress, sendCount, streamName]() {
        auto client = std::make_unique<ClientSC>(streamName);
        DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client->Subscribe("remoteSub", consumer));
        std::vector<Element> outElements;
        ASSERT_EQ(consumer->Receive(sendCount, 1000, outElements), Status::OK());
        ASSERT_EQ(outElements.size(), size_t(30));
        for (int i = 0; i < sendCount; i++) {
            ASSERT_EQ(outElements[i].id, static_cast<size_t>(i + 1));
            std::string actualData(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
            std::string curData = "Hello World" + std::to_string(i);
            ASSERT_EQ(curData, actualData);
        }
    });
    pool.Submit([&workerAddress, streamName]() {
        auto client = std::make_unique<ClientSC>(streamName);
        DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client->Subscribe("threadSub1", consumer));
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(producer));
        std::this_thread::sleep_for(std::chrono::milliseconds(11));
        client.reset();
    });
    pool.Submit([&workerAddress, streamName]() {
        auto client = std::make_unique<ClientSC>(streamName);
        DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client->Subscribe("threadSub2", consumer));
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(producer));
        std::this_thread::sleep_for(std::chrono::milliseconds(13));
        client.reset();
    });
}

TEST_F(ClientWorkerSCHeartbeatTest, TestClientWorkerTimeout)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
 
    // Create a client with user defined timeout
    int32_t timeout = 1000;
    ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                      .port = workerAddress.Port(),
                                      .connectTimeoutMs = timeout };
    connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
    connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
 
    auto client = std::make_shared<StreamClient>(connectOptions);
    client->Init();
 
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.CreateProducerImpl.sleep",
                                           "1*sleep(2000)"));
    Timer timer;

    // Create a producer
    std::shared_ptr<Producer> producer;
    ProducerConf defaultProducerConf;
    defaultProducerConf.maxStreamSize = 67108864;

    DS_ASSERT_NOT_OK(client->CreateProducer("testClientWorkerTimeout", producer, defaultProducerConf));
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cost: " << timeCost;

    // Test timeout
    ASSERT_TRUE(timeCost < 1200);
    std::this_thread::sleep_for(std::chrono::seconds(1));
}
}  // namespace st
}  // namespace datasystem

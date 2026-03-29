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

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include "client/stream_cache/pub_sub_utils.h"
#include "cluster/base_cluster.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/stream_config.h"

DS_DECLARE_string(log_dir);

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
constexpr uint32_t CONNECT_TIMEOUT_MS = 10000;
constexpr int WORKER_COUNT = 3;
class StreamDfxTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.numWorkers = WORKER_COUNT;
        opts.vLogLevel = logLevel;
        opts.workerGflagParams += FormatString(
            " -node_timeout_s=%d -node_dead_timeout_s=%d -client_reconnect_wait_s=2 -log_monitor=true "
            "-log_monitor_interval_ms=500",
            nodeTimeout, nodeDead);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client, uint32_t timeoutMs = CONNECT_TIMEOUT_MS)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions connectOptions;
        connectOptions.host = workerAddress.Host();
        connectOptions.port = workerAddress.Port();
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        connectOptions.connectTimeoutMs = timeoutMs;
        client = std::make_shared<StreamClient>(connectOptions);
        return client->Init();
    }

    Status CreateProducerAndConsumer(std::shared_ptr<StreamClient> &client,
                                     std::vector<std::pair<std::string, size_t>> producerDesc,
                                     std::vector<std::shared_ptr<Producer>> &producers,
                                     std::vector<std::pair<std::string, std::string>> consumerDesc,
                                     std::vector<std::shared_ptr<Consumer>> &consumers)
    {
        const int64_t delayFlushTime = 3 * 1000;  // 3s
        ProducerConf conf;
        conf.delayFlushTime = delayFlushTime;
        conf.maxStreamSize = TEST_STREAM_SIZE;
        conf.autoCleanup = true;
        for (const auto &kv : producerDesc) {
            for (size_t i = 0; i < kv.second; i++) {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(client->CreateProducer(kv.first, producer, conf));
                producers.emplace_back(producer);
            }
        }

        for (const auto &kv : consumerDesc) {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config(kv.second, SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(client->Subscribe(kv.first, config, consumer));
            consumers.emplace_back(consumer);
        }
        return Status::OK();
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
        ProducerConf conf;
        conf.maxStreamSize = TEST_STREAM_SIZE;
        return client->CreateProducer(streamName, producer, conf);
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

    void GetResMonitorLogInfo(int index, const std::string &fileName, std::vector<std::string> &infos)
    {
        std::string fullName = FormatString("%s/../worker%d/log/%s", FLAGS_log_dir.c_str(), index, fileName);
        std::ifstream ifs(fullName);
        ASSERT_TRUE(ifs.is_open());
        std::string line;
        std::streampos prev = ifs.tellg();
        std::streampos pos = ifs.tellg();
        // Get the last line
        while (std::getline(ifs, line)) {
            prev = pos;
            pos = ifs.tellg();
        }
        ifs.clear();
        ifs.seekg(prev);
        std::getline(ifs, line);
        infos = Split(line, " | ");
        const int ignoreCount = 7;
        ASSERT_TRUE(infos.size() == static_cast<size_t>(ResMetricName::RES_METRICS_END) + ignoreCount);
        infos.erase(infos.begin(), infos.begin() + ignoreCount);
    }

    const int nodeTimeout = 4;  // 4s;
    const int nodeDead = 6;     // 6s
    const int waitNodeTimeout = nodeTimeout + 2;
    const int waitNodeDead = nodeDead + 4;
    const int logLevel = 1;
    const int K_3 = 3;
    const int K_5000 = 5000;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamDfxTest, TestRemotePushTimeOut)
{
    LOG(INFO) << "TestRemotePushTimeOut start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::string streamName = "testRemotePushTO";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.RemoteSendOnePageView.end",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    const size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    CreateElement(testSize, element, writeElement);
    ASSERT_EQ(producer->Send(element), Status::OK());

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 100, outElements));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);
    LOG(INFO) << "TestRemotePushTimeOut finish!";
}

TEST_F(StreamDfxTest, TestMultiThreadsClientInit)
{
    std::shared_ptr<StreamClient> client;
    InitStreamClient(0, client);
    size_t threadNum = 100;
    ThreadPool threadPool(threadNum);

    for (size_t i = 0; i < threadNum; ++i) {
        threadPool.Execute([&client] { DS_ASSERT_OK(client->Init()); });
    }
}

TEST_F(StreamDfxTest, TestCreateProducerConsumerFail)
{
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "CreateProdConFail";
    DS_ASSERT_OK(datasystem::inject::Set("ClientBaseImpl.init_fail_before_cursor", "2*return(K_INVALID)"));
    DS_ASSERT_NOT_OK(CreateProducer(client1, streamName, producer));
    DS_ASSERT_NOT_OK(CreateConsumer(client1, streamName, "sub1", consumer));
    DS_ASSERT_OK(CreateProducer(client1, streamName, producer));
    DS_ASSERT_OK(CreateConsumer(client1, streamName, "sub1", consumer));
}

/*
AutoCleanup set to true. Create producer and consumer on same node.
CreateConsumer fails and prodcucer closes. CheckCount shows no producer
or consumer. AutoCleanup cleans stream metadata.
*/
TEST_F(StreamDfxTest, TestConsumerFailWithAutoDelete)
{
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    std::string streamName = "ConFailWithAutoDelete";
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(datasystem::inject::Set("ClientBaseImpl.init_fail_before_cursor", "1*return(K_INVALID)"));
    DS_ASSERT_NOT_OK(CreateConsumer(client1, streamName, "sub1", consumer));
    CheckCount(client1, streamName, 1, 0);
    DS_ASSERT_OK(producer->Close());
    CheckCount(client1, streamName, 0, 0);
}

/*
AutoCleanup set to true. Create producer and consumer on same node.
CreateProducer fails and consumer closes. CheckCount shows no producer
or consumer. AutoCleanup cleans stream metadata.
*/
TEST_F(StreamDfxTest, TestProducerFailWithAutoDelete)
{
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "ProdFailWithAutoDelete";
    DS_ASSERT_OK(CreateConsumer(client1, streamName, "sub1", consumer));
    DS_ASSERT_OK(datasystem::inject::Set("ClientBaseImpl.init_fail_before_cursor", "1*return(K_INVALID)"));
    DS_ASSERT_NOT_OK(CreateProducer(client1, streamName, producer));
    CheckCount(client1, streamName, 0, 1);
    DS_ASSERT_OK(consumer->Close());
    CheckCount(client1, streamName, 0, 0);
}

TEST_F(StreamDfxTest, TestProducerTimerQueue)
{
    DS_ASSERT_OK(datasystem::inject::Set("ProducerImpl.ExecAndCancelTimer.sleep", "1*sleep(1000)"));
    DS_ASSERT_OK(datasystem::inject::Set("ProducerImpl.ExecFlush.sleep", "1*sleep(5000)"));

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client1, "ProducerTimerQueue", producer));
    const size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    DS_ASSERT_OK(producer->Send(element));

    // In the destructor, before we cancel the timer in ExecAndCloseTimer(), we sleep 1 second to let the timer to
    // remove the task from the queue. Then the timer sleep for 5 seconds so that the destructor did the flush and the
    // producer deallocated. We expect the timer checks the producer still exist through weak pointer before calling
    // ExecFlush().
    producer.reset();
    LOG(INFO) << "producer destructed";
    // Sleep extra to ensure no segmentation fault
    const uint FIVE_SECS = 5;
    sleep(FIVE_SECS);
}

TEST_F(StreamDfxTest, TestMasterSubTimeout)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    uint32_t timeoutMs = 3000;
    DS_ASSERT_OK(InitClient(1, client1, timeoutMs));
    DS_ASSERT_OK(InitClient(2, client2, timeoutMs));
    std::string streamName = "testStream";
    std::shared_ptr<Consumer> consumer;

    for (int index = 0; index < WORKER_COUNT; index++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, index, "master.SubIncreaseNode.afterLock", "sleep(3000)"));
    }
    DS_ASSERT_NOT_OK(CreateConsumer(client1, streamName, streamName, consumer));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client2, streamName, producer));
}

TEST_F(StreamDfxTest, TestDiskFullWithAutoDelete)
{
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(1, client1));
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    std::string streamName = "DiskFullWithAutoDelete";
    client1->CreateProducer(streamName, producer, conf);
    sleep(1);
    std::vector<std::string> infos;
    GetResMonitorLogInfo(1, "resource.log", infos);
    int streamCountIdx = (int)ResMetricName::STREAM_COUNT - (int)ResMetricName::SHARED_MEMORY;
    ASSERT_EQ(std::stoi(infos[streamCountIdx]), 1);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.rocksdb.delete", "return(K_KVSTORE_ERROR)"));
    DS_ASSERT_OK(producer->Close());
    sleep(1);
    GetResMonitorLogInfo(1, "resource.log", infos);
    ASSERT_EQ(std::stoi(infos[streamCountIdx]), 0);

    DS_ASSERT_OK(cluster_->KillWorker(0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    sleep(3); // wait 3s for resource log flush
    GetResMonitorLogInfo(1, "resource.log", infos);
    ASSERT_EQ(std::stoi(infos[streamCountIdx]), 0);
}

class StreamDfxMultiTest : public StreamDfxTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 4;
        opts.numWorkers = workerCount;
        opts.masterIdx = 3;
        opts.enableDistributedMaster = "true";
    }

protected:
    Status SetupMulti(int i, std::vector<std::shared_ptr<Producer>> &producers,
                      std::vector<std::shared_ptr<Consumer>> &consumers, std::shared_ptr<StreamClient> &client1,
                      std::shared_ptr<StreamClient> &client2, std::shared_ptr<StreamClient> &client3,
                      std::shared_ptr<StreamClient> &client4, std::string strmName)
    {
        std::string streamName = strmName + std::to_string(i);
        const int TWO = 2;
        LOG(INFO) << FormatString("Setup Multi configuration %d!", i);
        switch (i) {
            case TWO:
                LOG(INFO) << "Config: W1: p1, W2: p2, W3: c1, W4: c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, {}, consumers));
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, {}, consumers));
                RETURN_IF_NOT_OK(
                    CreateProducerAndConsumer(client3, {}, producers, { { streamName, "sub1" } }, consumers));
                RETURN_IF_NOT_OK(
                    CreateProducerAndConsumer(client4, {}, producers, { { streamName, "sub2" } }, consumers));
                c1_location = TWO;
                break;
            case 1:
                LOG(INFO) << "Config: W1: p1 p2, W2: c1 c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client1, { { streamName, 2 } }, producers, {}, consumers));
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(
                    client2, {}, producers, { { streamName, "sub1" }, { streamName, "sub2" } }, consumers));
                c1_location = 1;
                break;
            case 0:
                LOG(INFO) << "Config: W1: p1 p2 c1 c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client1, { { streamName, 2 } }, producers,
                                                           { { streamName, "sub1" }, { streamName, "sub2" } },
                                                           consumers));
                c1_location = 0;
                break;
            default:
                LOG(INFO) << "No configuration";
                break;
        }
        return Status::OK();
    }

    Status MultiClientFaultHelper(int i, std::vector<std::shared_ptr<Producer>> &producers,
                                  std::vector<std::shared_ptr<Consumer>> &consumers,
                                  std::shared_ptr<StreamClient> &client1, std::shared_ptr<StreamClient> &client2,
                                  std::shared_ptr<StreamClient> &client3, std::shared_ptr<StreamClient> &client4,
                                  std::string strmName)
    {
        std::string streamName = strmName + std::to_string(i);
        const int TWO = 2;
        LOG(INFO) << FormatString("Setup Multi configuration %d!", i);
        switch (i) {
            case TWO:
                LOG(INFO) << "Config: W1: p1, W2: p2, W3: c1, W4: c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, {}, consumers));
                RETURN_IF_NOT_OK(
                    CreateProducerAndConsumer(client3, {}, producers, { { streamName, "sub1" } }, consumers));
                RETURN_IF_NOT_OK(
                    CreateProducerAndConsumer(client4, {}, producers, { { streamName, "sub2" } }, consumers));
                c1_location = TWO;
                break;
            case 1:
                LOG(INFO) << "Config: W1: p1 p2, W2: c1 c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, {}, consumers));
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(
                    client2, {}, producers, { { streamName, "sub1" }, { streamName, "sub2" } }, consumers));
                c1_location = 1;
                break;
            case 0:
                LOG(INFO) << "Config: W1: p1 p2 c1 c2";
                RETURN_IF_NOT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers,
                                                           { { streamName, "sub1" }, { streamName, "sub2" } },
                                                           consumers));
                c1_location = 0;
                break;
            default:
                break;
        }
        return Status::OK();
    }

    const uint32_t timeoutMs = 1000;
    const uint32_t DEFAULT_TIMEOUT_MS = 60'000;
    int c1_location = 0;
    std::string data = "Hello World";
    std::vector<Element> out;
    int TWO = 2, THREE = 3;
};

TEST_F(StreamDfxMultiTest, TestMultiBasic1)
{
    std::shared_ptr<StreamClient> client1, client2, client3, client4;
    std::string streamName = "TestMultiBasic1";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(TWO, client3));
    DS_ASSERT_OK(InitClient(THREE, client4));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    int NUM_CONFIGS = 3;
    for (int config = 0; config < NUM_CONFIGS; config++) {
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        SetupMulti(config, producers, consumers, client1, client2, client3, client4, streamName);
        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(consumers[0]->Receive(timeoutMs, out));
        DS_ASSERT_OK(consumers[1]->Receive(timeoutMs, out));
        DS_ASSERT_OK(producers[1]->Send(element));
        DS_ASSERT_OK(consumers[0]->Receive(timeoutMs, out));
        DS_ASSERT_OK(consumers[1]->Receive(timeoutMs, out));

        // Close producers/consumers
        DS_ASSERT_OK(producers[0]->Close());
        DS_ASSERT_OK(producers[1]->Close());
        DS_ASSERT_OK(consumers[0]->Close());
        DS_ASSERT_OK(consumers[1]->Close());
    }
}

TEST_F(StreamDfxMultiTest, TestMultiCloseProducer)
{
    std::shared_ptr<StreamClient> client1, client2, client3, client4;
    std::string streamName = "testMultiCloseProd";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(TWO, client3));
    DS_ASSERT_OK(InitClient(THREE, client4));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    int NUM_CONFIGS = 3;
    for (int config = 0; config < NUM_CONFIGS; config++) {
        // Close P1 during data transmission
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        SetupMulti(config, producers, consumers, client1, client2, client3, client4, streamName);
        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(producers[1]->Send(element));
        DS_ASSERT_OK(producers[0]->Close());

        // Assert normal functions
        DS_ASSERT_OK(consumers[0]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);
        DS_ASSERT_OK(consumers[1]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);

        DS_ASSERT_OK(producers[1]->Send(element));
        DS_ASSERT_OK(consumers[0]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);
        DS_ASSERT_OK(consumers[1]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);
        // Close producers/consumers due to out of scope
    }
}

TEST_F(StreamDfxMultiTest, TestMultiCloseConsumer)
{
    std::shared_ptr<StreamClient> client1, client2, client3, client4;
    std::string streamName = "testMultiCloseCon";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(TWO, client3));
    DS_ASSERT_OK(InitClient(THREE, client4));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    int NUM_CONFIGS = 3;
    for (int config = 0; config < NUM_CONFIGS; config++) {
        // Consumer C1 is closed during data transmission and receiving
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        SetupMulti(config, producers, consumers, client1, client2, client3, client4, streamName);
        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(consumers[1]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);
        DS_ASSERT_OK(producers[1]->Send(element));

        consumers[0]->Close();

        // Assert normal functions
        DS_ASSERT_OK(consumers[1]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);

        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(consumers[1]->Receive(1, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 1);
        // Close producers/consumers due to out of scope
    }
}

TEST_F(StreamDfxMultiTest, DISABLED_LEVEL1_TestMultiClientFault)
{
    std::shared_ptr<StreamClient> client1, client2, client3, client4;
    std::string streamName = "testMultiClientFaults";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(TWO, client3));
    DS_ASSERT_OK(InitClient(THREE, client4));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    int NUM_CONFIGS = 3;
    for (int config = 0; config < NUM_CONFIGS; config++) {
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        LOG(INFO) << FormatString("TestClientFault start configuration %d!", config);

        MultiClientFaultHelper(config, producers, consumers, client1, client2, client3, client4, streamName);
        auto pid = fork();
        if (pid == 0) {
            std::shared_ptr<StreamClient> client1a;
            DS_ASSERT_OK(InitClient(0, client1a));
            std::vector<std::shared_ptr<Producer>> p2;
            std::string streamName = "testMultiClientFaults" + std::to_string(config);
            DS_ASSERT_OK(CreateProducerAndConsumer(client1a, { { streamName, 1 } }, p2, {}, consumers));
            // Fake a crash point within producer
            datasystem::inject::Set("producer_insert", "1*abort()");
            DS_ASSERT_NOT_OK(p2[0]->Send(element));
            _exit(0);
        }
        int status;
        waitpid(pid, &status, 0);
        datasystem::inject::Clear("producer_insert");

        LOG(INFO) << FormatString("C1 located at %d", c1_location);
        // Verification
        for (const auto &consumer : consumers) {
            DS_ASSERT_OK(consumer->Receive(timeoutMs, out));
            DS_ASSERT_TRUE(out.size(), 0);
        }
        DS_ASSERT_OK(producers[0]->Send(element));

        for (const auto &consumer : consumers) {
            DS_ASSERT_OK(consumer->Receive(DEFAULT_TIMEOUT_MS, out));
            DS_ASSERT_TRUE(out.size(), 1);
        }
        LOG(INFO) << FormatString("TestClientFault configuration %d finished!", config);
        // All prod/cons are closed since out of scope
    }
}

TEST_F(StreamDfxMultiTest, TestMultiWorkerFault)
{
    std::string streamName = "testMultiWorkerFaults";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::shared_ptr<StreamClient> client3;
    std::shared_ptr<StreamClient> client4;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(TWO, client3));
    DS_ASSERT_OK(InitClient(THREE, client4));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    int NUM_CONFIGS = 3;
    for (int config = 0; config < NUM_CONFIGS; config++) {
        // Consumer C1 is closed during data transmission and receiving
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        SetupMulti(config, producers, consumers, client1, client2, client3, client4, streamName);
        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(producers[1]->Send(element));

        consumers[0]->Close();

        // Assert normal functions
        DS_ASSERT_OK(consumers[1]->Receive(timeoutMs, out));
        DS_ASSERT_OK(producers[1]->Send(element));
        DS_ASSERT_OK(producers[0]->Send(element));
        DS_ASSERT_OK(consumers[1]->Receive(TWO, timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), (unsigned int)TWO);
        // Close producers/consumers due to out of scope
    }
}

class StreamDfxWorkerCrashTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 3;
        opts.numWorkers = workerCount;
        opts.masterIdx = 2;
        opts.enableDistributedMaster = "false";
    }

protected:
    const int maxStreamSizeMb = 10;
};

TEST_F(StreamDfxWorkerCrashTest, DISABLED_TestWorkerCrashStopRemotePush)
{
    LOG(INFO) << "TestWorkerCrashStopRemotePush start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::string streamName = "testWkrCrashStopRemotePush";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSizeMb * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    const size_t testSize = 4ul * 1024ul;
    // Keep sending until out of memory
    size_t sendCount = 0;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    while (true) {
        Status rc = producer->Send(element);
        if (rc.IsOk()) {
            ++sendCount;
            continue;
        }
        ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
        break;
    }
    ASSERT_TRUE(sendCount > 0);
    LOG(INFO) << "Number of elements created: " << sendCount;
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    const int64_t timeoutMs = 1000;
    for (size_t i = 0; i < sendCount; i++) {
        DS_ASSERT_OK(producer->Send(element, timeoutMs));
    }
    LOG(INFO) << "TestWorkerCrashStopRemotePush finish!";
}

TEST_F(StreamDfxWorkerCrashTest, TestOneWorkerCrashAutoDelete)
{
    LOG(INFO) << "TestOneWorkerCrashAutoDelete start!";
    std::shared_ptr<StreamClient> client1;

    DS_ASSERT_OK(InitClient(0, client1));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testOneWkrCrashAutoDel";
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));

    CheckCount(client1, streamName, 1, 1);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, 0, 0);

    LOG(INFO) << "TestOneWorkerCrashAutoDelete finish!";
}

TEST_F(StreamDfxWorkerCrashTest, LEVEL1_TestOneWorkerCrash)
{
    LOG(INFO) << "TestOneWorkerCrash start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::shared_ptr<StreamClient> client3;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(2, client3));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testOneWkrCrashed";
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client3, { { streamName, 1 } }, producers, { { streamName, "sub3" } }, consumers));

    CheckCount(client1, streamName, 3, 3);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    CheckCount(client2, streamName, -1, 3);
    CheckCount(client3, streamName, -1, 3);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client2, streamName, 2, 2);
    CheckCount(client3, streamName, 2, 2);

    const size_t testSize = 1024;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    std::vector<Element> outElements;
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client2, streamName, "sub1", consumer));
    DS_ASSERT_OK(producers[1]->Send(element));
    DS_ASSERT_OK(producers[2]->Send(element));
    DS_ASSERT_OK(consumers[2]->Receive(2, 1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(2));
    outElements.clear();
    DS_ASSERT_OK(consumer->Receive(2, 1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(2));
    LOG(INFO) << "TestOneWorkerCrash finish!";
}

TEST_F(StreamDfxWorkerCrashTest, DISABLED_TestTwoWorkerCrash)
{
    LOG(INFO) << "TestTwoWorkerCrash start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::shared_ptr<StreamClient> client3;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(2, client3));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testTwoWkrCrashed";
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client3, { { streamName, 1 } }, producers, { { streamName, "sub3" } }, consumers));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.UpdateTopoNotification.setTimeout",
                                           "call(5000)"));

    CheckCount(client3, streamName, 3, 3);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    CheckCount(client3, streamName, -1, 3);

    // after worker0 start, master will clear metadata and notify to worker1 and worker2
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client3, streamName, -1, 1);

    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(CreateConsumer(client3, streamName, "sub1", consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateConsumer(client3, streamName, "sub2", consumer2));

    const size_t testSize = 1024;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);

    DS_ASSERT_OK(producers[2]->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer1->Receive(1, 1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));

    outElements.clear();
    DS_ASSERT_OK(consumer2->Receive(1, 1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));

    outElements.clear();
    DS_ASSERT_OK(consumers[2]->Receive(1, 1000, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));

    LOG(INFO) << "TestTwoWorkerCrash finish!";
}

TEST_F(StreamDfxWorkerCrashTest, DISABLED_LEVEL1_TestProducerWorkerCrashWhileConsumerReceive)
{
    LOG(INFO) << "LEVEL1_TestProducerWorkerCrashWhileConsumerReceive start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::string streamName = "ProdWkrCrashWhileConRecv";
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSizeMb * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;

    std::string data = "Hello";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    Status rc = consumer->Receive(1, -1, outElements);
    ASSERT_EQ(rc, Status::OK());
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(data, actualData);

    // Producer worker crashes. But the consumer is not aware till the nodeDead period passes.
    // Therefore, consumer makes a receive request and waits. The producer carsh report arrives after nodeDead period.
    // The consumer is unblocked and the error code for producer crashed is returned.
    ThreadPool threadPool(1);
    threadPool.Submit([this, consumer]() {
        std::vector<Element> outElements;
        Status rc = consumer->Receive(1, waitNodeDead * 1000, outElements);
        ASSERT_EQ(rc.GetCode(), K_SC_PRODUCER_NOT_FOUND);
    });

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    LOG(INFO) << "LEVEL1_TestProducerWorkerCrashWhileConsumerReceive finish!";
}

class StreamDfxMasterCrashTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 3;
        opts.masterIdx = 2;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "false";
    }
};

TEST_F(StreamDfxMasterCrashTest, LEVEL1_TestSameMetadata)
{
    LOG(INFO) << "TestSameMetadata start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "SameMeta", 1 } }, producers, { { "SameMeta", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "SameMeta", 1 } }, producers, { { "SameMeta", "sub2" } }, consumers));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 2);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(nodeTimeout));
    CheckCount(client1, "SameMeta", 2, 2);
    CheckCount(client2, "SameMeta", 2, 2);

    LOG(INFO) << "TestSameMetadata finish!";
}

TEST_F(StreamDfxMasterCrashTest, DISABLED_TestDiffMetadata)
{
    LOG(INFO) << "TestDiffMetadata start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "DiffMeta", 1 } }, producers, { { "DiffMeta", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "DiffMeta", 1 } }, producers, { { "DiffMeta", "sub2" } }, consumers));

    // close pub sub in worker 0, but not send to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    producers[0] = nullptr;
    consumers[0] = nullptr;
    CheckCount(client1, "DiffMeta", 2, 2);
    CheckCount(client2, "DiffMeta", 2, 2);

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 2);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    // Extend the sleep time for testcase stability purposes

    std::this_thread::sleep_for(std::chrono::seconds(nodeTimeout));
    CheckCount(client1, "DiffMeta", 1, 1);
    CheckCount(client2, "DiffMeta", 1, 1);

    LOG(INFO) << "TestDiffMetadata finish!";
}

TEST_F(StreamDfxMasterCrashTest, RecoveryAutoDeletePubSub)
{
    LOG(INFO) << "RecoveryAutoDeletePubSub start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testRecoveryAutoDelPubSub";

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));

    // close pub sub in worker 0, but not send to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    // Delete the producers and consumers on worker 0
    producers[0] = nullptr;
    consumers[0] = nullptr;
    // Master still thinks there are two producers and consumers because of the above injected actions
    CheckCount(client1, streamName, 2, 2);
    CheckCount(client2, streamName, 2, 2);

    // Remove remaining producer and consumer so when we restart we can invoke auto delete
    producers[1] = nullptr;
    consumers[1] = nullptr;

    cluster_->QuicklyShutdownWorker(2);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, 0, 0);
    CheckCount(client2, streamName, 0, 0);

    LOG(INFO) << "RecoveryAutoDeletePubSub finish!";
}

TEST_F(StreamDfxMasterCrashTest, LEVEL2_RecoveryAutoDeleteSub)
{
    LOG(INFO) << "RecoveryAutoDeleteSub start!";
    std::shared_ptr<StreamClient> client1;

    DS_ASSERT_OK(InitClient(0, client1));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testRecoverAutoDelSub";

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, {}, producers, { { streamName, "sub1" } }, consumers));

    // close pub sub in worker 0, but not send to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_OK)"));

    // Delete the producers and consumers on worker 0
    consumers[0] = nullptr;
    // Master still thinks there are two producers and consumers because of the above injected actions
    CheckCount(client1, streamName, 0, 1);

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 2);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, 0, 0);

    LOG(INFO) << "RecoveryAutoDeleteSub finish!";
}

TEST_F(StreamDfxMasterCrashTest, DISABLED_TestMasterAndClientCrash)
{
    LOG(INFO) << "TestMasterAndClientCrash start!";

    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "stream", 1 } }, producers, { { "stream", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "stream", 1 } }, producers, { { "stream", "sub2" } }, consumers));
    CheckCount(client1, "stream", 2, 2);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 2);
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));

    DS_ASSERT_OK(client1->ShutDown());

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client2, "stream", 1, 1);
    LOG(INFO) << "TestMasterAndClientCrash finish!";
}

TEST_F(StreamDfxMasterCrashTest, LEVEL1_TestCloseProducer)
{
    LOG(INFO) << "LEVEL1_TestCloseProducer start!";
    std::shared_ptr<StreamClient> client1;

    DS_ASSERT_OK(InitClient(0, client1));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "closeProdTest";

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 2);

    ASSERT_EQ(producers[0]->Close().GetCode(), K_RPC_UNAVAILABLE);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, 1, 1);
    DS_ASSERT_OK(producers[0]->Close());
    CheckCount(client1, streamName, 0, 1);
    LOG(INFO) << "LEVEL1_TestCloseProducer finish!";
}

TEST_F(StreamDfxMasterCrashTest, TestMasterFailRecoverMetaFromRocksDB)
{
    LOG(INFO) << "TestMasterFailRecoverMetaFromRocksDB start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "testMstrFailRecoverMetaRocksDB";
    std::string streamName2 = "testMstrFailRecoverMetaRocksDB_s2";
    // Do not store metadata on RocksDB
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, K_TWO,
                                           "master.RocksStreamMetaStore.DoNotAddPubSubMetadata", "6*return(K_OK)"));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName2, 1 } }, producers, { { streamName2, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, K_TWO, K_TWO);
    CheckCount(client1, streamName2, 1, 1);
    CheckCount(client2, streamName, K_TWO, K_TWO);
    LOG(INFO) << "TestMasterFailRecoverMetaFromRocksDB finish!";
}

TEST_F(StreamDfxMasterCrashTest, LEVEL1_TestMasterAndWorkerLostMetadata)
{
    std::string streamName = "testMstrWrkrLostMeta";
    LOG(INFO) << "TestMasterAndWorkerLostMetadata start!";
    auto pid = fork();
    if (pid == 0) {
        // Do not store metadata on RocksDB
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, K_TWO,
                                               "master.RocksStreamMetaStore.DoNotAddPubSubMetadata", "2*return(K_OK)"));

        cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO);
        DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
        DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO));
    }
    ASSERT_TRUE(pid > 0);
    std::shared_ptr<StreamClient> client1;
    const int timeoutMs = 2000;
    DS_ASSERT_OK(InitClient(0, client1, timeoutMs));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    CheckCount(client1, streamName, 1, 1);

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    int status;
    waitpid(pid, &status, 0);

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, streamName, 0, 0);

    LOG(INFO) << "TestMasterAndWorkerLostMetadata finish!";
}

TEST_F(StreamDfxMasterCrashTest, LEVEL1_TestMasterAndSubscriberRestart)
{
    LOG(INFO) << "TestMasterAndWorkerRestart start!";
    std::shared_ptr<StreamClient> client1, client2, client3;
    std::string streamName = "testMstrAndSubRestart";

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    // Do not store metadata on RocksDB
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, K_TWO,
                                           "master.RocksStreamMetaStore.DoNotAddPubSubMetadata", "2*return(K_OK)"));

    std::vector<std::shared_ptr<Producer>> producers1, producers2;
    std::vector<std::shared_ptr<Consumer>> consumers1, consumers2;

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers1, {}, consumers1));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, {}, producers2, { { streamName, "sub1" } }, consumers2));
    CheckCount(client1, streamName, 1, 1);

    ThreadPool pool(K_TWO);
    auto fut1 = pool.Submit([this]() { cluster_->ShutdownNode(ClusterNodeType::WORKER, 1); });
    auto fut2 = pool.Submit([this]() { cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO); });
    fut1.get();
    fut2.get();

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));

    fut1 = pool.Submit([this]() { cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1); });
    fut2 = pool.Submit([this]() { cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO); });
    fut1.get();
    fut2.get();
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    CheckCount(client2, streamName, 1, 0);
    DS_ASSERT_OK(InitClient(1, client3));

    DS_ASSERT_OK(CreateProducerAndConsumer(client3, {}, producers2, { { streamName, "sub2" } }, consumers2));
    std::string data = "This is some data";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    producers1[0]->Send(element);
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumers2[1]->Receive(1, 10000, outElements));
    ASSERT_EQ(outElements.size(), (size_t)1);
    LOG(INFO) << "TestMasterAndWorkerRestart finish!";
}

TEST_F(StreamDfxMasterCrashTest, LEVEL2_TestMasterAndPublisherRestart)
{
    LOG(INFO) << "TestMasterAndWorkerRestart start!";
    std::shared_ptr<StreamClient> client1, client2, client3;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    // Do not store metadata on RocksDB
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, K_TWO,
                                           "master.RocksStreamMetaStore.DoNotAddPubSubMetadata", "2*return(K_OK)"));

    std::vector<std::shared_ptr<Producer>> producers1, producers2;
    std::vector<std::shared_ptr<Consumer>> consumers1, consumers2;

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { "MasterPublisherRestart", 1 } }, producers1, {}, consumers1));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, {}, producers2, { { "MasterPublisherRestart", "sub1" } }, consumers2));
    CheckCount(client1, "MasterPublisherRestart", 1, 1);

    ThreadPool pool(K_TWO);
    auto fut1 = pool.Submit([this]() { cluster_->ShutdownNode(ClusterNodeType::WORKER, 0); });
    auto fut2 = pool.Submit([this]() { cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO); });
    fut1.get();
    fut2.get();
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    fut1 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO)); });
    fut2 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0)); });
    fut1.get();
    fut2.get();
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    CheckCount(client2, "MasterPublisherRestart", 0, 1);
    DS_ASSERT_OK(InitClient(1, client3));

    DS_ASSERT_OK(CreateProducerAndConsumer(client3, { { "MasterPublisherRestart", 1 } }, producers1, {}, consumers1));
    std::string data = "This is some data";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    producers1[1]->Send(element);
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumers2[0]->Receive(1, 10000, outElements));
    ASSERT_EQ(outElements.size(), (size_t)1);
    LOG(INFO) << "TestMasterAndWorkerRestart finish!";
}

TEST_F(StreamDfxMasterCrashTest, DISABLED_LEVEL1_TestQueryMetaProducerNotFound)
{
    // This testcase aims to test a bug fix where producer pb can be empty
    // at QueryMeta request if the client id for the producer is not found
    LOG(INFO) << "LEVEL1_TestQueryMetaProducerNotFound start!";
    const int masterIdx = 2;

    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::string streamName = "testQueryMetaProdNotFound";

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers,
                                           { { streamName, "subscription1" } }, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers,
                                           { { streamName, "subscription2" } }, consumers));
    CheckCount(client1, streamName, K_TWO, K_TWO);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, masterIdx);
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));

    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "GetProducerConsumerMetadata.NotFound", "1*call()"));
    // Cleanup rocksdb so stream fields can be updated at reconciliation
    std::string rocksPath =
        cluster_->GetRootDir() + "/worker" + std::to_string(masterIdx) + "/rocksdb/stream_meta_data";
    LOG(INFO) << "Remove rocksdb at path " << rocksPath;
    DS_ASSERT_OK(RemoveAll(rocksPath));

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, masterIdx, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, masterIdx));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    static_cast<ExternalCluster *>(cluster_.get())->KillWorker(masterIdx);
    sleep(1);
    static_cast<ExternalCluster *>(cluster_.get())->StartNode(ClusterNodeType::WORKER, masterIdx, "");
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    CheckCount(client1, streamName, K_TWO, K_TWO);
    LOG(INFO) << "LEVEL1_TestQueryMetaProducerNotFound finish!";
}

TEST_F(StreamDfxMasterCrashTest, TestMetadataNodeFault)
{
    // After producer P1 and consumer C1 are created, the node where the metadata is located is faulty.
    // Data receiving and sending are not affected.
    // Producer or consumer fails to close until the node where the metadata resides recovers.

    LOG(INFO) << "TestMetadataNodeFault start!";

    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::string streamName = "testMetaNodeFault";
    std::string streamName2 = "testMetaNodeFault_s2";

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));

    CheckCount(client1, streamName, 1, 1);

    // Injection for logging metadata not found
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "GetProducerConsumerMetadata.NotFound", "10*call()"));
    // no trigger
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, K_TWO, "master.sc.close_producer_error", "1*return()"));

    // Shut down metadata node
    cluster_->ShutdownNode(ClusterNodeType::WORKER, K_TWO);

    // Assert that data send and receive is not affected
    std::string data = "This is some data";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    producers[0]->Send(element);
    std::vector<Element> outElements;
    const int64_t timeoutMs = 1000;
    DS_ASSERT_OK(consumers[0]->Receive(1, timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);

    // Assert that closing does not work
    DS_ASSERT_NOT_OK(producers[0]->Close());
    DS_ASSERT_NOT_OK(consumers[0]->Close());

    // Restart:
    LOG(INFO) << "Restarting metadata node";
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, K_TWO, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, K_TWO));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName2, 1 } }, producers, { { streamName2, "sub2" } }, consumers));

    // Assert that closing works
    LOG(INFO) << "Can close producer and consumer now";
    DS_ASSERT_OK(producers[0]->Close());
    DS_ASSERT_OK(consumers[0]->Close());
    DS_ASSERT_OK(producers[1]->Close());
    DS_ASSERT_OK(consumers[1]->Close());

    CheckCount(client1, streamName, 0, 0);
    LOG(INFO) << "TestMetadataNodeFault finish!";
}

class StreamDfxHeartbeatTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 3;
        opts.masterIdx = 2;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "false";
        opts.disableRocksDB = false;
    }
};

TEST_F(StreamDfxHeartbeatTest, LEVEL1_TestWorkerCrashTimeout)
{
    LOG(INFO) << "LEVEL1_TestWorkerCrashTimeout start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testWrkrCrashTimeout";

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));

    CheckCount(client1, streamName, 2, 2);
    cluster_->QuicklyShutdownWorker(0);
    CheckCount(client2, streamName, -1, 2);

    // sleep until master clear the worker metadata.
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeDead));
    CheckCount(client2, streamName, -1, 1);

    const int K_TWO = 2;
    ThreadPool pool(K_TWO);
    cluster_->QuicklyShutdownWorker(1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    auto fut1 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0)); });
    auto fut2 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1)); });
    fut1.get();
    fut2.get();
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client2, streamName, -1, 0);

    // sleep until node times out, and check that consumer can still be created
    std::shared_ptr<Consumer> consumer1;
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateConsumer(client1, streamName, "sub1", consumer1));
    DS_ASSERT_OK(CreateConsumer(client2, streamName, "sub2", consumer2));
    LOG(INFO) << "LEVEL1_TestWorkerCrashTimeout finish!";
}

TEST_F(StreamDfxHeartbeatTest, LEVEL1_TestMasterAndWorkerCrashNotStartWorker)
{
    LOG(INFO) << "LEVEL1_TestMasterAndWorkerCrashNotStartWorker start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    const int timeoutMs = 2000;
    DS_ASSERT_OK(InitClient(0, client1, timeoutMs));
    DS_ASSERT_OK(InitClient(1, client2, timeoutMs));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "MasterWorkerCrashNotStartWorker";
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));

    client1.reset();
    const int workerIdx = 2;
    static_cast<ExternalCluster *>(cluster_.get())->KillWorker(workerIdx);
    static_cast<ExternalCluster *>(cluster_.get())->KillWorker(0);
    static_cast<ExternalCluster *>(cluster_.get())->StartNode(ClusterNodeType::WORKER, 2, "");
    CheckCount(client2, streamName, -1, 2);

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeDead));
    CheckCount(client2, streamName, -1, 1);
    static_cast<ExternalCluster *>(cluster_.get())->KillWorker(workerIdx);
    LOG(INFO) << "LEVEL1_TestMasterAndWorkerCrashNotStartWorker finish!";
}

TEST_F(StreamDfxHeartbeatTest, LEVEL1_TestWorkerToMasterTimeout)
{
    LOG(INFO) << "LEVEL1_TestWorkerToMasterTimeout start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "TestWorkerMasterTimeout";
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { streamName, 1 } }, producers, { { streamName, "sub2" } }, consumers));

    CheckCount(client1, streamName, 2, 2);

    // heartbeat timeout and node dead.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "heartbeat.sleep", "1*sleep(10000)"));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeDead));
    CheckCount(client1, streamName, 1, 1);

    const size_t testSize = 1024;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);

    DS_ASSERT_OK(producers[1]->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumers[1]->Receive(1, 100, outElements));
    LOG(INFO) << "LEVEL1_TestWorkerToMasterTimeout finish!";
}

class StreamDfxTopoTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 4;
        opts.numWorkers = workerCount;
        opts.masterIdx = 3;
        opts.workerGflagParams = "-node_timeout_s=2 -node_dead_timeout_s=60";
        opts.disableRocksDB = false;
    }

    void SetUp() override
    {
        StreamDfxTest::SetUp();
        int index = 0;
        DS_ASSERT_OK(InitClient(index++, client1_));
        DS_ASSERT_OK(InitClient(index++, client2_));
        DS_ASSERT_OK(InitClient(index++, client3_));
    }

    void TearDown() override
    {
        client1_ = nullptr;
        client2_ = nullptr;
        client3_ = nullptr;
        StreamDfxTest::TearDown();
    }

protected:
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

    void waitAbort(uint32_t idx);
    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
    std::shared_ptr<StreamClient> client3_;
    const int K_TWO = 2, K_3 = 3, K_5 = 5, K_200 = 200;
};

TEST_F(StreamDfxTopoTest, TestCreateConsumerTimeout)
{
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client1_, "testCreateConsumerTimeout", "sub", consumer));
}

TEST_F(StreamDfxTopoTest, TestCreateProducerConsumer)
{
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(CreateConsumer(client1_, "TestDfxCreateProducerConsumer", "sub", consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(CreateProducer(client1_, "TestDfxCreateProducerConsumer", producer));

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());

    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateConsumer(client1_, "TestDfxCreateProducerConsumer2", "sub", consumer2));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(CreateProducer(client1_, "TestDfxCreateProducerConsumer2", producer2));

    DS_ASSERT_OK(producer2->Send(element));
    DS_ASSERT_OK(consumer2->Receive(1, 0, outElements));
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(producer2->Close());
}

TEST_F(StreamDfxTopoTest, LEVEL1_TestTopoChangeWhenWorkerTimeout)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.UpdateTopoNotification.setTimeout",
                                           "call(2000)"));

    std::shared_ptr<Producer> p1w1;
    std::shared_ptr<Producer> p1w2;
    std::shared_ptr<Producer> p1w3;
    std::string streamName = "TopoWhenWorkerTimeout";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, p1w1));
    DS_ASSERT_OK(CreateProducer(client2_, streamName, p1w2));
    DS_ASSERT_OK(CreateProducer(client3_, streamName, p1w3));

    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub-w1", c1w1));

    // Sleep 10s when worker 0 send heartbeat message.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "heartbeat.sleep", "1*sleep(10000)"));
    const uint32_t sleepTime = 18 * 1000;  // 18s
    auto heartBeartRecoverTime = std::chrono::system_clock::now() + std::chrono::milliseconds(sleepTime);
    std::shared_ptr<Consumer> c1w2;
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "sub-w2", c1w2));

    // After 3 seconds, the master thinks the worker1 has timeout.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::shared_ptr<Consumer> c1w3;
    DS_ASSERT_OK(CreateConsumer(client3_, streamName, "sub-w3", c1w3));
    DS_ASSERT_OK(c1w2->Close());
    DS_ASSERT_OK(c1w3->Close());

    std::shared_ptr<Consumer> c2w2;
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "sub-w22", c2w2));
    std::shared_ptr<Consumer> c2w3;
    DS_ASSERT_OK(CreateConsumer(client3_, streamName, "sub-w23", c2w3));
    while (std::chrono::system_clock::now() <= heartBeartRecoverTime) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    DS_ASSERT_OK(p1w1->Send(element));
    DS_ASSERT_OK(p1w2->Send(element));
    DS_ASSERT_OK(p1w3->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(c1w1->Receive(K_3, K_5000, outElements));
    ASSERT_EQ(outElements.size(), 3ul);
    DS_ASSERT_OK(c2w2->Receive(K_3, K_5000, outElements));
    ASSERT_EQ(outElements.size(), 3ul);
    DS_ASSERT_OK(c2w3->Receive(K_3, K_5000, outElements));
    ASSERT_EQ(outElements.size(), 3ul);

    DS_ASSERT_OK(p1w1->Close());
    DS_ASSERT_OK(p1w2->Close());
    DS_ASSERT_OK(p1w3->Close());
    DS_ASSERT_OK(c1w1->Close());
    DS_ASSERT_OK(c2w2->Close());
    DS_ASSERT_OK(c2w3->Close());
    DS_ASSERT_OK(client1_->DeleteStream(streamName));
}

TEST_F(StreamDfxTopoTest, LEVEL1_TestNotAllowDeleteStreamIfExistPendingNotify)
{
    std::string streamName = "NotAllowDeleteStream";
    // The testcase tests that if there is pending async notification to send, the delete stream will not succeed.
    // It is done via having NotifyDelConsumer UpdateTopoNotification request go through RPC unavailable.
    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub-w1", c1w1));

    std::shared_ptr<Producer> p1w2;
    DS_ASSERT_OK(CreateProducer(client2_, streamName, p1w2));

    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 3, "master.SendPendingNotification", "1*sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "heartbeat.sleep", "1*sleep(10000)"));
    const uint32_t sleepTime = 20 * 1000;  // 20s
    auto heartBeartRecoverTime = std::chrono::system_clock::now() + std::chrono::milliseconds(sleepTime);

    // After 5 seconds, the master thinks the worker1 has timeout.
    std::this_thread::sleep_for(std::chrono::seconds(5));

    DS_ASSERT_OK(c1w1->Close());
    DS_ASSERT_OK(p1w2->Close());
    DS_ASSERT_NOT_OK(client2_->DeleteStream(streamName));

    while (std::chrono::system_clock::now() <= heartBeartRecoverTime) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    DS_ASSERT_OK(client2_->DeleteStream(streamName));
}

TEST_F(StreamDfxTopoTest, DISABLED_TestContinueSendNotifyAfterMasterRestart)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 3, "master.UpdateTopoNotification.setTimeout",
                                           "call(2000)"));

    std::shared_ptr<Producer> p1w1;
    DS_ASSERT_OK(CreateProducer(client1_, "test-stream", p1w1));
    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_OK(CreateConsumer(client1_, "test-stream", "sub-w1", c1w1));

    // Sleep 10s when worker 0 send heartbeat message.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "heartbeat.sleep", "1*sleep(10000)"));
    const uint32_t sleepTime = 20 * 1000;  // 20s
    auto heartBeartRecoverTime = std::chrono::system_clock::now() + std::chrono::milliseconds(sleepTime);

    // After 3 seconds, the master thinks worker1 has timeout.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::shared_ptr<Consumer> c1w2;
    DS_ASSERT_OK(CreateConsumer(client2_, "test-stream", "sub-w2", c1w2));
    std::shared_ptr<Producer> p1w2;
    DS_ASSERT_OK(CreateProducer(client2_, "test-stream", p1w2));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 3);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 3, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 3));

    while (std::chrono::system_clock::now() <= heartBeartRecoverTime) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::string str = "hello hello";
    Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(str.data())), str.length());

    std::vector<Element> outElements;
    DS_ASSERT_OK(p1w1->Send(element));
    DS_ASSERT_OK(c1w2->Receive(1, 1000, outElements));
    ASSERT_EQ(outElements.size(), 1ul);

    DS_ASSERT_OK(p1w2->Send(element));
    DS_ASSERT_OK(c1w1->Receive(1, 1000, outElements));
    ASSERT_EQ(outElements.size(), 1ul);

    DS_ASSERT_OK(p1w1->Close());
    DS_ASSERT_OK(c1w1->Close());
    DS_ASSERT_OK(p1w2->Close());
    DS_ASSERT_OK(c1w2->Close());

    DS_ASSERT_OK(client1_->DeleteStream("test-stream"));
}

TEST_F(StreamDfxTopoTest, DISABLED_TestMasterCrashWhenCreateProducer)
{
    std::shared_ptr<Consumer> c1w2;
    DS_ASSERT_OK(CreateConsumer(client2_, "test-stream", "sub-w2", c1w2));
    std::shared_ptr<Consumer> c1w3;
    DS_ASSERT_OK(CreateConsumer(client3_, "test-stream", "sub-w3", c1w3));

    DS_ASSERT_OK(inject::Set("rpc_util.retry_on_rpc_error_by_count", "1*call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 3,
                                           "master.PubIncreaseNodeImpl.afterSendNotification", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CreateProducer.beforeSendToMaster",
                                           "call(K_OK)"));
    std::shared_ptr<Producer> p1w1;
    DS_ASSERT_NOT_OK(CreateProducer(client1_, "test-stream", p1w1));

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 3, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 3));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    DS_ASSERT_OK(c1w2->Close());
    DS_ASSERT_OK(c1w3->Close());
    DS_ASSERT_OK(client2_->DeleteStream("test-stream"));
}

void StreamDfxTopoTest::waitAbort(uint32_t idx)
{
    while (cluster_->CheckWorkerProcess(idx)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(K_200));
    }
}

TEST_F(StreamDfxTopoTest, LEVEL2_TestMasterCrashWhenCloseProducer)
{
    std::string streamName = "MasterCrashWhenCloseProd";
    std::shared_ptr<Producer> p1w1;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, p1w1));

    std::shared_ptr<Consumer> c1w2;
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "sub-w2", c1w2));
    std::shared_ptr<Consumer> c1w3;
    DS_ASSERT_OK(CreateConsumer(client3_, streamName, "sub-w3", c1w3));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 3, "master.PubDecreaseNode.afterSendNotification",
                                           "abort()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster", "call(K_OK)"));

    DS_ASSERT_NOT_OK(p1w1->Close());

    (void)cluster_->KillWorker(K_3);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 3, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 3));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    DS_ASSERT_OK(p1w1->Close());
    DS_ASSERT_OK(c1w2->Close());
    DS_ASSERT_OK(c1w3->Close());
    DS_ASSERT_OK(client2_->DeleteStream(streamName));
}

TEST_F(StreamDfxTopoTest, LEVEL1_TestMasterCrashWhenSubscribe)
{
    std::string streamName = "MasterCrashWhenSub";
    std::shared_ptr<Producer> p1w2;
    DS_ASSERT_OK(CreateProducer(client2_, streamName, p1w2));
    std::shared_ptr<Producer> p1w3;
    DS_ASSERT_OK(CreateProducer(client3_, streamName, p1w3));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 3,
                                           "master.SubIncreaseNodeImpl.afterSendNotification", "abort()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.Subscribe.beforeSendToMaster", "call(K_OK)"));
    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_NOT_OK(CreateConsumer(client1_, streamName, "sub-w1", c1w1));

    (void)cluster_->KillWorker(K_3);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 3, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 3));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    DS_ASSERT_OK(p1w2->Close());
    DS_ASSERT_OK(p1w3->Close());
    DS_ASSERT_OK(client2_->DeleteStream(streamName));
}

TEST_F(StreamDfxTopoTest, LEVEL1_TestMasterCrashWhenCloseConsumer)
{
    std::string streamName = "MasterCrashWhenCloseCon";
    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub-w1", c1w1));
    std::shared_ptr<Producer> p1w2;
    DS_ASSERT_OK(CreateProducer(client2_, streamName, p1w2));
    std::shared_ptr<Producer> p1w3;
    DS_ASSERT_OK(CreateProducer(client3_, streamName, p1w3));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 3, "master.SubDecreaseNode.afterSendNotification",
                                           "abort()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster", "call(K_OK)"));
    DS_ASSERT_NOT_OK(c1w1->Close());

    (void)cluster_->KillWorker(K_3);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 3, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 3));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    DS_ASSERT_OK(c1w1->Close());
    DS_ASSERT_OK(p1w2->Close());
    DS_ASSERT_OK(p1w3->Close());
    DS_ASSERT_OK(TryAndDeleteStream(client2_, streamName));
}

TEST_F(StreamDfxTopoTest, LEVEL1_TestWorkerRestartThenClosePubSub)
{
    std::shared_ptr<Producer> p1w1;
    DS_ASSERT_OK(CreateProducer(client1_, "WorkerRestartClosePubSub", p1w1));
    std::shared_ptr<Consumer> c1w1;
    DS_ASSERT_OK(CreateConsumer(client1_, "WorkerRestartClosePubSub", "sub-w1", c1w1));

    std::shared_ptr<Producer> p2w2;
    DS_ASSERT_OK(CreateProducer(client2_, "WorkerRestartClosePubSub", p2w2));
    std::shared_ptr<Consumer> c2w2;
    DS_ASSERT_OK(CreateConsumer(client2_, "WorkerRestartClosePubSub", "sub-w2", c2w2));

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));

    DS_ASSERT_OK(p1w1->Close());
    DS_ASSERT_OK(c1w1->Close());
    DS_ASSERT_OK(client1_->DeleteStream("WorkerRestartClosePubSub"));
}

class StreamDfxDistMasterCrashTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        opts.masterIdx = 2;
        opts.numWorkers = workerCount_;
        opts.enableDistributedMaster = "true";
    }

protected:
    const uint32_t workerCount_ = 3;
};

TEST_F(StreamDfxDistMasterCrashTest, TestSameMetadata)
{
    LOG(INFO) << "TestSameMetadata start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(K_TWO, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "stream", 1 } }, producers, { { "stream", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "stream", 1 } }, producers, { { "stream", "sub2" } }, consumers));

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client1, "stream", K_TWO, K_TWO);
    CheckCount(client2, "stream", K_TWO, K_TWO);

    LOG(INFO) << "TestSameMetadata finish!";
}

TEST_F(StreamDfxDistMasterCrashTest, DISABLED_TestDiffMetadata)
{
    LOG(INFO) << "TestDiffMetadata start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(K_TWO, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "stream", 1 } }, producers, { { "stream", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "stream", 1 } }, producers, { { "stream", "sub2" } }, consumers));

    // close pub sub in worker 0, but not send to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_OK)"));
    producers[0] = nullptr;
    consumers[0] = nullptr;
    CheckCount(client1, "stream", K_TWO, K_TWO);
    CheckCount(client2, "stream", K_TWO, K_TWO);

    // Here stream is always hashed to worker 1
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    // Extend the sleep time for testcase stability purposes
    const int WAIT_NODE_READY_TIME = 10;
    std::this_thread::sleep_for(std::chrono::seconds(WAIT_NODE_READY_TIME));
    CheckCount(client1, "stream", 1, 1);
    CheckCount(client2, "stream", 1, 1);

    LOG(INFO) << "TestDiffMetadata finish!";
}

TEST_F(StreamDfxDistMasterCrashTest, DISABLED_TestMasterAndClientCrash)
{
    LOG(INFO) << "TestMasterAndClientCrash start!";

    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(K_TWO, client2));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { "stream", 1 } }, producers, { { "stream", "sub1" } }, consumers));
    DS_ASSERT_OK(
        CreateProducerAndConsumer(client2, { { "stream", 1 } }, producers, { { "stream", "sub2" } }, consumers));
    CheckCount(client1, "stream", K_TWO, K_TWO);
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseConsumer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CloseProducer.beforeSendToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));

    DS_ASSERT_OK(client1->ShutDown());

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client2, "stream", 1, 1);
    LOG(INFO) << "TestMasterAndClientCrash finish!";
}

TEST_F(StreamDfxDistMasterCrashTest, TestCloseProducer)
{
    LOG(INFO) << "TestCloseProducer start!";
    std::shared_ptr<StreamClient> client1;

    DS_ASSERT_OK(InitClient(0, client1));

    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "testCloseProd";

    DS_ASSERT_OK(
        CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, { { streamName, "sub1" } }, consumers));

    for (uint32_t i = 0; i < workerCount_; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "master.CloseProducerImpl",
                                               "1*return(K_RPC_UNAVAILABLE)"));
    }

    ASSERT_EQ(producers[0]->Close().GetCode(), K_RPC_UNAVAILABLE);

    CheckCount(client1, streamName, 1, 1);
    DS_ASSERT_OK(producers[0]->Close());
    CheckCount(client1, streamName, 0, 1);
    LOG(INFO) << "TestCloseProducer finish!";
}

class StreamDistDfxTopoTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 2;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-node_timeout_s=2 -node_dead_timeout_s=60 -v=2";
    }

    void SetUp() override
    {
        StreamDfxTest::SetUp();
        int index = 0;
        DS_ASSERT_OK(InitClient(index++, client1_));
        DS_ASSERT_OK(InitClient(index++, client2_));
    }

    void TearDown() override
    {
        client1_ = nullptr;
        client2_ = nullptr;
        StreamDfxTest::TearDown();
    }

    template <typename Func>
    void UntilTrueOrTimeout(Func &&func, uint64_t timeoutMs)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string value;
            if (func()) {
                return;
            }
            const int interval = 1000;  // 1000ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(false) << "Timeout";
    }

protected:
    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
};

TEST_F(StreamDistDfxTopoTest, DISABLED_LEVEL1_TestWorkerExistsMetaAndStart)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.UpdateTopoNotification.setTimeout",
                                           "call(2000)"));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "master.UpdateTopoNotification.setTimeout",
                                           "call(2000)"));
    std::shared_ptr<Producer> p1w1;
    DS_ASSERT_OK(CreateProducer(client1_, "test-stream", p1w1));
    std::shared_ptr<Consumer> c1w2;
    DS_ASSERT_OK(CreateConsumer(client2_, "test-stream", "sub-w2-1", c1w2));

    // Sleep 10s when worker 0 send heartbeat message.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "heartbeat.sleep", "1*sleep(10000)"));
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, "-inject_actions=worker.InitRing:call()"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    std::shared_ptr<Consumer> c2w2;
    DS_ASSERT_OK(CreateConsumer(client2_, "test-stream", "sub-w2-2", c2w2));
}

TEST_F(StreamDistDfxTopoTest, TestWorkerRestartRetryCheckMeta)
{
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    const int streamCount = 10;
    for (int i = 0; i < streamCount; i++) {
        std::string streamName = "test-stream-" + std::to_string(i);
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(CreateConsumer(client1_, streamName, "sub", consumer));
        producers.emplace_back(std::move(producer));
        consumers.emplace_back(std::move(consumer));
    }

    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(
        cluster_->StartNode(ClusterNodeType::WORKER, 1,
                            "-inject_actions=worker.MasterRemoteWorkerSCApi.QueryMetadata:3*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    int maxTimeout = 10000;  // 10s.
    for (int i = 0; i < streamCount; i++) {
        std::string streamName = "test-stream-" + std::to_string(i);
        UntilTrueOrTimeout(
            [this, &streamName] {
                uint64_t gProducerNum = 0;
                uint64_t gConsumerNum = 0;
                LOG_IF_ERROR(client1_->QueryGlobalProducersNum(streamName, gProducerNum),
                             "QueryGlobalProducersNum failed");
                LOG_IF_ERROR(client1_->QueryGlobalConsumersNum(streamName, gConsumerNum),
                             "QueryGlobalConsumersNum failed");
                return gProducerNum == 1 && gConsumerNum == 1;
            },
            maxTimeout);
    }
}

class StreamDfxSingleProducerMultiConsumerTest : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        const uint32_t workerCount = 3;
        opts.masterIdx = 2;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
    }

protected:
    int timeOut = 1000;
    std::string data = "Hello World";
};

/*
On same node. Create 1 producer 2 consumers. producer is faulty on send through injection.
consumers should not be able to receive anything. Producer can send normally after
injection cleared. Consumer will receive after.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, SameNodeProducerClientFault)
{
    LOG(INFO) << "SameNodeProducerClientFault start!";
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "SameNodeProducerClientFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers,
                                           { { streamName, "sub1" }, { streamName, "sub2" } }, consumers));
    std::vector<Element> outElements;
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    std::thread sendThread([&]() {
        datasystem::inject::Set("producer_insert", "1*return(K_INVALID)");
        DS_ASSERT_NOT_OK(producers[0]->Send(element));
    });
    for (const auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Receive(timeOut, outElements));
        DS_ASSERT_TRUE(outElements.size(), 0);
    }
    sendThread.join();
    datasystem::inject::Clear("producer_insert");

    DS_ASSERT_OK(producers[0]->Send(element));
    for (const auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Receive(timeOut, outElements));
        DS_ASSERT_TRUE(outElements.size(), 1);
    }

    LOG(INFO) << "SameNodeProducerClientFault finish!";
}

/*
On same node. Create 1 producer 2 consumers. 1 Consumer is faulty on receive through
injection. Consumer2 should be able to receive still.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, SameNodeOneConsumerClientFault)
{
    LOG(INFO) << "SameNodeOneConsumerClientFault start!";
    std::shared_ptr<StreamClient> client1, client2, client3;
    DS_ASSERT_OK(InitClient(0, client1));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "SameNodeOneConsumerClientFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers,
                                           { { streamName, "sub1" }, { streamName, "sub2" } }, consumers));
    std::vector<Element> outElements1;
    std::vector<Element> outElements2;
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    std::thread sendThread([&]() { DS_ASSERT_OK(producers[0]->Send(element)); });
    datasystem::inject::Set("consumerImpl.receive.fail", "1*return(K_INVALID)");
    DS_ASSERT_NOT_OK(consumers[0]->Receive(timeOut, outElements1));
    DS_ASSERT_OK(consumers[1]->Receive(timeOut, outElements2));

    sendThread.join();
    DS_ASSERT_TRUE(outElements1.size(), 0);
    DS_ASSERT_TRUE(outElements2.size(), 1);

    LOG(INFO) << "SameNodeOneConsumerClientFault finish!";
}

/*
On same node. Create 1 producer 2 consumers. Both consumers are faulty on receive through
injection. Producer sends, but both consumers do not receive. Can receive normally after
injection clear.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, SameNodeBothConsumerClientFault)
{
    LOG(INFO) << "SameNodeBothConsumerClientFault start!";
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "stream_BothConsumerClientFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers,
                                           { { streamName, "sub1" }, { streamName, "sub2" } }, consumers));
    std::vector<Element> outElements;
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    std::thread sendThread([&]() { DS_ASSERT_OK(producers[0]->Send(element)); });
    datasystem::inject::Set("consumerImpl.receive.fail", "2*return(K_INVALID)");
    for (const auto &consumer : consumers) {
        DS_ASSERT_NOT_OK(consumer->Receive(timeOut, outElements));
    }
    sendThread.join();
    datasystem::inject::Clear("consumerImpl.receive.fail");

    for (const auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Receive(timeOut, outElements));
        DS_ASSERT_TRUE(outElements.size(), 1);
    }

    LOG(INFO) << "SameNodeBothConsumerClientFault finish!";
}

/*
On different nodes. Create 1 producer 2 consumemrs on 3 seperate node. Node that has
producer is faulty and is shutdown. producer is not able to send. restart the node.
global prod count should be 0 and consumers cannot receive anything. creating new
producer after node restart would work fine.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, DiffNodeProducerWorkerFault)
{
    LOG(INFO) << "DiffNodeProducerWorkerFault start!";
    std::shared_ptr<StreamClient> client1, client2, client3;
    int idx = 2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(idx, client3));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "DiffNodeProducerWorkerFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, {}, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, { {} }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client3, { {} }, producers, { { streamName, "sub2" } }, consumers));
    std::vector<Element> outElements;
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    DS_ASSERT_NOT_OK(producers[0]->Send(element));

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    int consumerCheck = 2;
    CheckCount(client1, streamName, 0, consumerCheck);
    for (const auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Receive(timeOut, outElements));
        DS_ASSERT_TRUE(outElements.size(), 0);
    }

    LOG(INFO) << "DiffNodeProducerWorkerFault finish!";
}

/*
On different nodes. Create 1 producer 2 consumemrs on 3 seperate node. Node that has
first consumer is faulty and is shutdown. consumer[0] is cannot receive. restart the node.
global consumer count should be 1 and only one consumer can receive. creating new
consumer after node restart would work fine.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, DISABLED_DiffNodeOneConsumerWorkerFault)
{
    LOG(INFO) << "DiffNodeOneConsumerWorkerFault start!";
    std::shared_ptr<StreamClient> client1, client2, client3;
    int idx = 2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(idx, client3));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "DiffNodeOneConsumerWorkerFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, {}, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, { {} }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client3, { {} }, producers, { { streamName, "sub2" } }, consumers));
    std::vector<Element> outElements;
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producers[0]->Send(element));

    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
    DS_ASSERT_NOT_OK(consumers[0]->Receive(timeOut, outElements));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client2, streamName, 1, 1);
    DS_ASSERT_OK(consumers[1]->Receive(timeOut, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);

    LOG(INFO) << "DiffNodeOneConsumerWorkerFault finish!";
}

/*
On different nodes. Create 1 producer 2 consumers on 3 seperate node. both node that
consumer is faulty and is shutdown. consumers cannot receive. restart the node.
global consumer count should be 1 and only one consumer can receive. creating new
consumers after node restart would work fine.
*/
TEST_F(StreamDfxSingleProducerMultiConsumerTest, DISABLED_DiffNodeBothConsumerWorkerFault)
{
    LOG(INFO) << "DiffNodeBothConsumerWorkerFault start!";
    std::shared_ptr<StreamClient> client1, client2, client3;
    int idx = 2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    DS_ASSERT_OK(InitClient(idx, client3));
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::string streamName = "DiffNodeBothConsumerWorkerFault";
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName, 1 } }, producers, {}, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, { {} }, producers, { { streamName, "sub1" } }, consumers));
    DS_ASSERT_OK(CreateProducerAndConsumer(client3, { {} }, producers, { { streamName, "sub2" } }, consumers));
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producers[0]->Send(element));
    ThreadPool pool(idx);
    auto fut1 = pool.Submit([this, consumers]() {
        DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
        std::vector<Element> outElements;
        DS_ASSERT_NOT_OK(consumers[0]->Receive(timeOut, outElements));
    });
    auto fut2 = pool.Submit([this, consumers]() {
        DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(2));
        std::vector<Element> outElements1;
        DS_ASSERT_NOT_OK(consumers[1]->Receive(timeOut, outElements1));
    });
    fut1.get();
    fut2.get();
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, ""));
    fut1 = pool.Submit([this, consumers]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1)); });
    fut2 = pool.Submit([this, consumers]() { DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2)); });
    fut1.get();
    fut2.get();
    std::this_thread::sleep_for(std::chrono::seconds(waitNodeTimeout));
    CheckCount(client3, streamName, 1, 0);

    LOG(INFO) << "DiffNodeBothConsumerWorkerFault finish!";
}

// DFX testcases for Multiple-Producer and Single-Consumer
class StreamDfxClientCrashMPSC : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamDfxTest::SetClusterSetupOptions(opts);
        opts.numWorkers = workerCount_;
    }

protected:
    const uint32_t workerCount_ = 3;
};

TEST_F(StreamDfxClientCrashMPSC, DISABLED_SameNodeProducerProcessCrash)
{
    std::string streamName("stream1");

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Create Producer1
    auto pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> client1;
        DS_ASSERT_OK(InitClient(0, client1));

        std::shared_ptr<Producer> Producer1;
        DS_ASSERT_OK(CreateProducer(client1, streamName, Producer1));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(Producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);

    // Create Producer2
    pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> client2;
        DS_ASSERT_OK(InitClient(0, client2));
        std::shared_ptr<Producer> Producer2;
        DS_ASSERT_OK(CreateProducer(client2, streamName, Producer2));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(Producer2->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    int status;
    waitpid(pid, &status, 0);

    // Create Consumer1
    std::shared_ptr<StreamClient> client3;
    DS_ASSERT_OK(InitClient(0, client3));
    std::shared_ptr<Consumer> Consumer1;
    DS_ASSERT_OK(CreateConsumer(client3, streamName, "sub1", Consumer1));

    std::vector<Element> outElements;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(Consumer1->Receive(timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), 0);
    DS_ASSERT_OK(Consumer1->Close());
}

TEST_F(StreamDfxClientCrashMPSC, DISABLED_SameNodeConsumerProcessCrash)
{
    std::string streamName("testSameNodeConProcessCrash");

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));

    // Create Consumer1
    auto pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> client2;
        DS_ASSERT_OK(InitClient(0, client2));
        std::shared_ptr<Consumer> Consumer1;
        DS_ASSERT_OK(CreateConsumer(client2, streamName, "sub1", Consumer1));
        std::vector<Element> outElements;

        const int SLEEP_TIME = 1000;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));

        // Fake a crash point within consumer after it gets the DataPage from worker
        datasystem::inject::Set("consumer_after_get_datapage", "abort()");
        const uint32_t timeoutMs = 2000;
        DS_ASSERT_NOT_OK(Consumer1->Receive(timeoutMs, outElements));
        DS_ASSERT_TRUE(outElements.size(), 0);
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);

    // Create Producer1 and Producer2
    std::shared_ptr<Producer> Producer1;
    DS_ASSERT_OK(CreateProducer(client1, streamName, Producer1));
    std::shared_ptr<Producer> Producer2;
    DS_ASSERT_OK(CreateProducer(client1, streamName, Producer2));

    uint64_t totalConsumerNum = 0;
    while (totalConsumerNum == 0) {
        client1->QueryGlobalConsumersNum(streamName, totalConsumerNum);
    }

    int threadNum = 2;
    ThreadPool pool(threadNum);
    auto fut1 = pool.Submit([this, Producer1, element]() {
        Producer1->Send(element);
        Producer1->Close();
    });
    auto fut2 = pool.Submit([this, Producer2, element]() {
        Producer2->Send(element);
        Producer2->Close();
    });

    int status;
    waitpid(pid, &status, 0);
    fut1.get();
    fut2.get();
}

class StreamDfxWorkerCrashMPSC : public StreamDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = numWorkers_;
        opts.numEtcd = numEtcd_;
        opts.numRpcThreads = numRpcThreads_;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        StreamDfxTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        client1_ = nullptr;
        client2_ = nullptr;
        client3_ = nullptr;
        StreamDfxTest::TearDown();
    }

protected:
    void InitTest()
    {
        int workerIndex = 0;
        InitClient(workerIndex++, client1_);
        InitClient(workerIndex++, client2_);
        InitClient(workerIndex, client3_);
    }

    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;

    // Cluster config
    int numWorkers_ = 3;
    int numEtcd_ = 1;
    int numRpcThreads_ = 0;
};

TEST_F(StreamDfxWorkerCrashMPSC, CrossNodeProducerWorkerCrash)
{
    std::string stream1("CrossNodeProducerWorkerCrash");

    std::shared_ptr<Producer> Producer1;
    DS_ASSERT_OK(CreateProducer(client1_, stream1, Producer1));
    std::shared_ptr<Producer> Producer2;
    DS_ASSERT_OK(CreateProducer(client2_, stream1, Producer2));
    std::shared_ptr<Consumer> Consumer1;
    DS_ASSERT_OK(CreateConsumer(client3_, stream1, "sub1", Consumer1));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    int threadNum = 4;
    ThreadPool pool(threadNum);
    uint32_t workerIndex = 0;

    cluster_->QuicklyShutdownWorker(workerIndex);

    // Set pause at send
    auto fut2 = pool.Submit([this, Producer2, element]() { DS_ASSERT_OK(Producer2->Send(element)); });
    datasystem::inject::Set("ProducerImpl.beforeCreateWritePage", "sleep(3000)");
    auto fut1 = pool.Submit([this, Producer1, element]() { DS_ASSERT_NOT_OK(Producer1->Send(element)); });
    auto fut3 = pool.Submit([this, Consumer1]() {
        uint32_t timeoutMs = 5000;
        std::vector<Element> outElements;
        DS_ASSERT_OK(Consumer1->Receive(timeoutMs, outElements));
        DS_ASSERT_TRUE(outElements.size(), 1);
    });

    // Wait for all send/receive to finish
    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(StreamDfxWorkerCrashMPSC, DISABLED_CrossNodeBothProducerWorkerCrash)
{
    std::string stream1("CrossNodeBothProducerWorkerCrash");

    std::shared_ptr<Producer> Producer1;
    DS_ASSERT_OK(CreateProducer(client1_, stream1, Producer1));
    std::shared_ptr<Producer> Producer2;
    DS_ASSERT_OK(CreateProducer(client2_, stream1, Producer2));
    std::shared_ptr<Consumer> Consumer1;
    DS_ASSERT_OK(CreateConsumer(client3_, stream1, "sub1", Consumer1));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    int threadNum = 5;
    ThreadPool pool(threadNum);
    uint32_t workerIndex = 0;

    // Close workers of both Producer1 and Producer2
    auto fut = pool.Submit([this, workerIndex]() { cluster_->QuicklyShutdownWorker(workerIndex); });
    workerIndex++;
    auto fut0 = pool.Submit([this, workerIndex]() { cluster_->QuicklyShutdownWorker(workerIndex); });
    fut.get();
    fut0.get();

    // Set pause at send
    datasystem::inject::Set("ProducerImpl.beforeCreateWritePage", "sleep(3000)");
    auto fut1 = pool.Submit([this, Producer1, element]() { DS_ASSERT_NOT_OK(Producer1->Send(element)); });
    auto fut2 = pool.Submit([this, Producer2, element]() { DS_ASSERT_NOT_OK(Producer2->Send(element)); });
    auto fut3 = pool.Submit([this, Consumer1]() {
        uint32_t timeoutMs = 3000;
        std::vector<Element> outElements;
        DS_ASSERT_OK(Consumer1->Receive(timeoutMs, outElements));
        DS_ASSERT_TRUE(outElements.size(), 0);
    });

    // Wait for all send/receive to return
    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(StreamDfxWorkerCrashMPSC, LEVEL1_CrossNodeConsumerWorkerCrash)
{
    std::string stream1("CrossNodeConsumerWorkerCrash");

    std::shared_ptr<Producer> Producer1;
    DS_ASSERT_OK(CreateProducer(client1_, stream1, Producer1));
    std::shared_ptr<Producer> Producer2;
    DS_ASSERT_OK(CreateProducer(client2_, stream1, Producer2));
    std::shared_ptr<Consumer> Consumer1;
    DS_ASSERT_OK(CreateConsumer(client3_, stream1, "sub1", Consumer1));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    int threadNum = 3;
    ThreadPool pool(threadNum);
    uint32_t workerIndex = 2;

    // shutdown the consumer worker
    cluster_->QuicklyShutdownWorker(workerIndex);

    // Set pause at receive/send
    auto fut1 = pool.Submit([this, Producer1, element]() { DS_ASSERT_OK(Producer1->Send(element)); });
    auto fut2 = pool.Submit([this, Producer2, element]() { DS_ASSERT_OK(Producer2->Send(element)); });
    auto fut3 = pool.Submit([this, Consumer1]() {
        uint32_t timeoutMs = 5000;
        std::vector<Element> outElements;
        DS_ASSERT_NOT_OK(Consumer1->Receive(timeoutMs, outElements));
        ASSERT_TRUE(outElements.size() < 2);
    });

    // Wait for all send/receive to finish
    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(StreamDfxWorkerCrashMPSC, CrossNodeNetworkIssueBetweenWorkers)
{
    std::string stream1("CrossNodeNetworkIssueBetweenWorkers");

    std::shared_ptr<Producer> Producer1;
    DS_ASSERT_OK(CreateProducer(client1_, stream1, Producer1));
    std::shared_ptr<Producer> Producer2;
    DS_ASSERT_OK(CreateProducer(client2_, stream1, Producer2));
    std::shared_ptr<Consumer> Consumer1;
    DS_ASSERT_OK(CreateConsumer(client3_, stream1, "sub1", Consumer1));

    // Simulate lost rpc request
    int workerIndex = 2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, workerIndex, "PushElementsCursors.begin", "sleep(10000)"));
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    int threadNum = 3;
    ThreadPool pool(threadNum);

    // Set pause after PushElementsCursors being sent

    auto fut1 = pool.Submit([this, Producer1, element]() { DS_ASSERT_OK(Producer1->Send(element)); });
    auto fut2 = pool.Submit([this, Producer2, element]() { DS_ASSERT_OK(Producer2->Send(element)); });
    auto fut3 = pool.Submit([this, Consumer1]() {
        uint32_t timeoutMs = 5000;
        std::vector<Element> outElements;
        DS_ASSERT_OK(Consumer1->Receive(timeoutMs, outElements));
        ASSERT_TRUE(outElements.size() == 0);
    });

    // Wait for all send/receive to finish
    fut1.get();
    fut2.get();
    fut3.get();
}

}  // namespace st
}  // namespace datasystem

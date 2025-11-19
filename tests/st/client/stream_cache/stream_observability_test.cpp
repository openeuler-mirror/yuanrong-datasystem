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
 * Description: Test observability.
 */

#include <gtest/gtest.h>
#include <re2/re2.h>
#ifdef BUILD_OBSERVABILITY
#include <nlohmann/json.hpp>
#endif

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/common/metrics/res_metric_collector.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
#ifdef BUILD_OBSERVABILITY
using json = nlohmann::json;
#endif

class StreamObservabilityTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;

    void SetUp() override;

    void TearDown() override;

    static std::string streamName_;

protected:
    void GetResMonitorLogInfo(int index, const std::string &fileName, std::vector<std::string> &infos);
    std::string GetMetric(const std::vector<std::string> &workerMetrics, ResMetricName metric);
    Status Produce(std::shared_ptr<Producer> &producer, std::string producerName, int numEle, uint64_t eleSz,
                   int timeout = 0);
    Status CreateProducerAndConsumer(const std::shared_ptr<StreamClient> &client,
                                     std::vector<std::pair<std::string, size_t>> producerDesc,
                                     std::vector<std::shared_ptr<Producer>> &producers,
                                     std::vector<std::pair<std::string, std::string>> consumerDesc,
                                     std::vector<std::shared_ptr<Consumer>> &consumers, bool autoCleanup);

    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const int NUM_WORKER = 2;
    const int V_LEVEL = 2;
    const int SLEEP_TIME = 2;
    const int DEFAULT_NUM_ELEMENT = 20;
    const int TEST_ELEMENT_SIZE = 2 * KB - 128;
    const int MAX_STREAM_SIZE = 2 * MB;
    const int DELAY_FLUSH_TIME = 3000;
};
std::string StreamObservabilityTest::streamName_ = "stream";

void StreamObservabilityTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = NUM_WORKER;
    opts.enableDistributedMaster = "false";
    opts.workerGflagParams = " -page_size=" + std::to_string(PAGE_SIZE)
                             + " -log_monitor=true -log_monitor_interval_ms=2000 -shared_memory_size_mb=64";
    opts.numRpcThreads = 0;
    opts.vLogLevel = V_LEVEL;
    SCClientCommon::SetClusterSetupOptions(opts);
}

void StreamObservabilityTest::SetUp()
{
    ExternalClusterTest::SetUp();
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    // Worker 1.
    InitStreamClient(0, w1Client_);
    // Worker 2.
    InitStreamClient(1, w2Client_);
    defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
}

void StreamObservabilityTest::TearDown()
{
    w1Client_ = nullptr;
    w2Client_ = nullptr;
    ExternalClusterTest::TearDown();
}

void StreamObservabilityTest::GetResMonitorLogInfo(int index, const std::string &fileName,
                                                   std::vector<std::string> &infos)
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
};

std::string StreamObservabilityTest::GetMetric(const std::vector<std::string> &workerMetrics, ResMetricName metric)
{
    int index = (int)metric - (int)ResMetricName::SHARED_MEMORY;
    return workerMetrics[index];
}

Status StreamObservabilityTest::CreateProducerAndConsumer(const std::shared_ptr<StreamClient> &client,
                                                          std::vector<std::pair<std::string, size_t>> producerDesc,
                                                          std::vector<std::shared_ptr<Producer>> &producers,
                                                          std::vector<std::pair<std::string, std::string>> consumerDesc,
                                                          std::vector<std::shared_ptr<Consumer>> &consumers,
                                                          bool autoCleanup)
{
    ProducerConf conf;
    conf.delayFlushTime = DELAY_FLUSH_TIME;
    conf.pageSize = PAGE_SIZE;  // 4K
    conf.maxStreamSize = MAX_STREAM_SIZE;
    conf.autoCleanup = autoCleanup;
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
        RETURN_IF_NOT_OK(client->Subscribe(kv.first, config, consumer, false));
        consumers.emplace_back(consumer);
    }
    return Status::OK();
}

Status StreamObservabilityTest::Produce(std::shared_ptr<Producer> &producer, std::string producerName, int numEle,
                                        uint64_t eleSz, int timeout)
{
    Status stat = Status::OK();
    ElementGenerator elementGenerator(eleSz, eleSz);
    auto strs = elementGenerator.GenElements(producerName, numEle, 1);
    Status rc;

    for (int i = 0; i < numEle; i++) {
        if (timeout) {
            rc = producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size()), timeout);
        } else {
            rc = producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size()));
        }
        if (rc.IsError()) {
            stat = rc;
        }
    }
    return stat;
}

TEST_F(StreamObservabilityTest, DISABLED_StreamMetricsLog)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName_, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName_, config, consumer));
    const uint32_t eleSz = 512;
    const uint32_t eleNum = 1000;
    ElementGenerator elementGenerator(eleSz);
    auto strs = elementGenerator.GenElements("producer1", eleNum, 1);

    auto sender = [&producer, &strs]() {
        for (uint32_t i = 0; i < eleNum; i++) {
            DS_ASSERT_OK(producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
        }
    };

    const uint32_t recvNum = 100;
    std::vector<Element> outElements;
    auto recver = [eleNum, recvNum, &consumer, &outElements]() {
        for (size_t i = 0; i < eleNum / recvNum; ++i) {
            DS_ASSERT_OK(consumer->Receive(recvNum, 5000, outElements));
            ASSERT_EQ(outElements.size(), recvNum);
            DS_ASSERT_OK(consumer->Ack(recvNum));
        }
    };

    std::thread sendThr(sender);
    std::thread recvThr(recver);
    sendThr.join();
    recvThr.join();

    std::vector<std::string> infos;
    GetResMonitorLogInfo(1, "resource.log", infos);
    // expected pattern/format of stream metrics
    re2::RE2 strNumPattern("[0-9 ]+");
    ASSERT_TRUE(re2::RE2::FullMatch(infos[static_cast<size_t>(ResMetricName::STREAM_COUNT)], strNumPattern));
}

TEST_F(StreamObservabilityTest, StreamCount)
{
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    const int streamNum = 2;
    DS_ASSERT_OK(CreateProducerAndConsumer(
        w1Client_, { { "stream1", 3 } }, producers,
        { { "stream1", "sub1" }, { "stream2", "sub1" }, { "stream1", "sub2" }, { "stream2", "sub2" } }, consumers,
        true));

    DS_ASSERT_OK(
        CreateProducerAndConsumer(w2Client_, { { "stream2", 3 }, { "stream3", 1 } }, producers, {}, consumers, true));

    sleep(SLEEP_TIME);

    std::vector<std::string> worker0Metrics;
    GetResMonitorLogInfo(1, "resource.log", worker0Metrics);

    std::vector<std::string> worker1Metrics;
    GetResMonitorLogInfo(1, "resource.log", worker1Metrics);
    ASSERT_EQ(std::stoi(GetMetric(worker0Metrics, ResMetricName::STREAM_COUNT)), streamNum);
    ASSERT_EQ(std::stoi(GetMetric(worker1Metrics, ResMetricName::STREAM_COUNT)), streamNum);

    for (auto &producer : producers) {
        DS_ASSERT_OK(producer->Close());
    }
    for (auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Close());
    }
    sleep(SLEEP_TIME);

    GetResMonitorLogInfo(1, "resource.log", worker0Metrics);
    GetResMonitorLogInfo(1, "resource.log", worker1Metrics);
    ASSERT_EQ(std::stoi(GetMetric(worker0Metrics, ResMetricName::STREAM_COUNT)), 0);
    ASSERT_EQ(std::stoi(GetMetric(worker1Metrics, ResMetricName::STREAM_COUNT)), 0);
}

TEST_F(StreamObservabilityTest, StreamSharedMemory)
{
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::string> worker0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    const int theoreticalMem = 5 * 40 * KB;
    const int memoryLimit = 64 * MB;

    DS_ASSERT_OK(CreateProducerAndConsumer(w1Client_, { { "stream1", 3 }, { "stream2", 2 } }, producers,
                                           { { "stream1", "sub1" }, { "stream2", "sub1" }, { "stream2", "sub2" } },
                                           consumers, false));

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }
    sleep(SLEEP_TIME);

    GetResMonitorLogInfo(0, "resource.log", worker0Metrics);
    // TotalStreamMemoryUsed is real memory size allocated, might be larger than theoretical
    std::vector<std::string> metrics = Split(GetMetric(worker0Metrics, ResMetricName::SHARED_MEMORY), "/");
    int streamMemoryUsage = std::stoi(metrics[4]);
    int streamMemoryLimit = std::stoi(metrics[5]);
    ASSERT_TRUE(streamMemoryUsage >= theoreticalMem);
    ASSERT_TRUE(streamMemoryLimit <= memoryLimit);
}
#ifdef BUILD_OBSERVABILITY
class StreamYrObservabilityTest : public StreamObservabilityTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;
    void GetObservabilityMetricsLog0(int index);
    void GetObservabilityMetricsLog1(int index);
};

void StreamYrObservabilityTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = NUM_WORKER;
    opts.enableDistributedMaster = "false";
    opts.workerGflagParams = " -page_size=" + std::to_string(PAGE_SIZE)
                             + " -log_monitor=true -log_monitor_interval_ms=100 -log_monitor_exporter=yr_file_exporter";
    opts.numRpcThreads = 0;
    opts.vLogLevel = V_LEVEL;
    SCClientCommon::SetClusterSetupOptions(opts);
}

void StreamYrObservabilityTest::GetObservabilityMetricsLog0(int index)
{
    std::string fullName =
        FormatString("%s/../worker%d/log/observability/yr_metrics.data", FLAGS_log_dir.c_str(), index);
    std::ifstream ifs(fullName, std::ios::binary);
    ASSERT_TRUE(ifs.is_open());
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        ASSERT_TRUE(!line.empty());
        json jsonLine = json::parse(line);
        const std::string &name = jsonLine["name"];
        const std::string &value = jsonLine["value"];
        if (name == "STREAM_COUNT_STREAM_COUNT" && std::stoi(value) > 0) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
}

TEST_F(StreamYrObservabilityTest, StreamMetricsLog0)
{
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName_, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub0", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName_, config, consumer));
    const uint32_t eleSz = 512;
    const uint32_t eleNum = 1000;
    ElementGenerator elementGenerator(eleSz);
    auto strs = elementGenerator.GenElements("producer1", eleNum, 1);

    auto sender = [&producer, &strs]() {
        for (uint32_t i = 0; i < eleNum; i++) {
            DS_ASSERT_OK(producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
        }
    };

    const uint32_t recvNum = 100;
    std::vector<Element> outElements;
    auto recver = [eleNum, recvNum, &consumer, &outElements]() {
        for (size_t i = 0; i < eleNum / recvNum; ++i) {
            DS_ASSERT_OK(consumer->Receive(recvNum, 5000, outElements));
            ASSERT_EQ(outElements.size(), recvNum);
            DS_ASSERT_OK(consumer->Ack(recvNum));
        }
    };

    std::thread sendThr(sender);
    std::thread recvThr(recver);
    sendThr.join();
    recvThr.join();

    GetObservabilityMetricsLog0(0);
    GetObservabilityMetricsLog0(1);
}

TEST_F(StreamYrObservabilityTest, StreamMetricsLog1)
{
    std::shared_ptr<Producer> producer0;
    std::string streamName0 = streamName_ + "0";
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName0, producer0, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer0;
    DS_ASSERT_OK(w2Client_->Subscribe(streamName0, SubscriptionConfig("sub0", SubscriptionType::STREAM), consumer0));

    std::shared_ptr<Producer> producer1;
    std::string streamName1 = streamName_ + "1";
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName1, producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe(streamName1, SubscriptionConfig("sub1", SubscriptionType::STREAM), consumer1));

    const uint32_t eleSz = 512;
    const uint32_t eleNum = 1000;
    ElementGenerator elementGenerator(eleSz);
    auto strs = elementGenerator.GenElements("producer1", eleNum, 1);

    auto sender = [&producer0, &producer1, &strs]() {
        for (uint32_t i = 0; i < eleNum; i++) {
            DS_ASSERT_OK(producer0->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
            DS_ASSERT_OK(producer1->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
        }
    };

    const uint32_t recvNum = 100;
    std::vector<Element> outElements0;
    std::vector<Element> outElements1;
    auto recver = [eleNum, recvNum, &consumer0, &consumer1, &outElements0, &outElements1]() {
        for (size_t i = 0; i < eleNum / recvNum; ++i) {
            DS_ASSERT_OK(consumer0->Receive(recvNum, 5000, outElements0));
            ASSERT_EQ(outElements0.size(), recvNum);
            DS_ASSERT_OK(consumer0->Ack(recvNum));
            DS_ASSERT_OK(consumer1->Receive(recvNum, 5000, outElements1));
            ASSERT_EQ(outElements1.size(), recvNum);
            DS_ASSERT_OK(consumer1->Ack(recvNum));
        }
    };

    std::thread sendThr(sender);
    std::thread recvThr(recver);
    sendThr.join();
    recvThr.join();

    GetObservabilityMetricsLog0(0);
    GetObservabilityMetricsLog0(1);
}
#endif
}  // namespace st
}  // namespace datasystem

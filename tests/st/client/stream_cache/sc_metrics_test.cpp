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
 * Description: Unit test for stream cache metrics
 */

#include <mutex>
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DECLARE_string(log_dir);

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class SCMetricsTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.numWorkers = WORKER_NUM;
        opts.vLogLevel = VLOG_LEVEL;
        opts.masterIdx = 0;
        std::string workerGflags = "-sc_local_cache_memory_size_mb=20 -log_monitor=true -sc_metrics_log_interval_s="
                                   + std::to_string(PRINT_INTERVAL) + " -sc_cache_pages=" + std::to_string(CACHE_PAGES);
        opts.workerGflagParams = workerGflags;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        InitStreamClient(index, client);
        return Status::OK();
    }

    Status CreateProducerAndConsumer(std::shared_ptr<StreamClient> &client,
                                     std::vector<std::pair<std::string, size_t>> producerDesc,
                                     std::vector<std::shared_ptr<Producer>> &producers,
                                     std::vector<std::pair<std::string, std::string>> consumerDesc,
                                     std::vector<std::shared_ptr<Consumer>> &consumers, bool autoCleanup)
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

    Status CloseAllProducerAndConsumer(std::vector<std::shared_ptr<Producer>> &producers,
                                       std::vector<std::shared_ptr<Consumer>> &consumers)
    {
        for (auto &producer : producers) {
            RETURN_IF_NOT_OK(producer->Close());
        }
        for (auto &consumer : consumers) {
            RETURN_IF_NOT_OK(consumer->Close());
        }
        return Status::OK();
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

    Status Produce(std::shared_ptr<Producer> &producer, std::string producerName, int numEle, uint64_t eleSz,
                   int timeout = 0)
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

    Status ConsumeAll(std::shared_ptr<Consumer> &consumer, int timeout = 5000, bool checkFIFO = true,
                      uint64_t *res = nullptr, int producerNum = 1, bool ack = true)
    {
        std::vector<Element> outElements;
        size_t expectNum = DEFAULT_NUM_ELEMENT * producerNum;
        RETURN_IF_NOT_OK(consumer->Receive(expectNum, timeout, outElements));
        if (ack) {
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
        }
        LOG(INFO) << FormatString("Stream Consumer Receive %d elements.", outElements.size());
        std::unordered_map<std::string, uint64_t> seqNoMap;
        uint64_t eleTotalSz = 0;
        for (const auto &element : outElements) {
            ElementView view(std::string((const char *)element.ptr, element.size));
            RETURN_IF_NOT_OK(view.VerifyIntegrity());
            if (checkFIFO) {
                RETURN_IF_NOT_OK(view.VerifyFifo(seqNoMap, 0));
            }
            eleTotalSz += element.size;
        }
        if (res != nullptr) {
            *res = eleTotalSz;
        }
        return Status::OK();
    }

    void VerifyAllStreamMetrics(const std::vector<std::string> &workerMetrics,
                                const std::vector<std::vector<std::string>> &allScMetrics,
                                const std::vector<std::string> &expectedWM,
                                const std::vector<std::vector<std::string>> expectedScM)
    {
        ASSERT_EQ(workerMetrics.size(), expectedWM.size());
        for (size_t i = 0; i < workerMetrics.size(); i++) {
            ASSERT_EQ(workerMetrics[i], expectedWM[i]);
        }

        ASSERT_EQ(allScMetrics.size(), expectedScM.size());
        for (size_t i = 0; i < allScMetrics.size(); i++) {
            auto scMetrics = allScMetrics[i];
            auto expected = expectedScM[i];
            ASSERT_EQ(scMetrics.size(), expected.size());
            for (size_t j = 0; j < scMetrics.size(); j++) {
                ASSERT_EQ(scMetrics[j], expected[j]);
            }
        }
    }

    void GetStreamMetrics(int index, const std::string &fileName,
                          std::unordered_map<std::string, std::vector<std::string>> &allScMetrics)
    {
        allScMetrics.clear();
        std::string fullName = FormatString("%s/../worker%d/log/%s", FLAGS_log_dir.c_str(), index, fileName);
        std::ifstream ifs(fullName);
        ASSERT_TRUE(ifs.is_open());
        std::string line;
        std::streampos metric_start_pos = ifs.tellg();
        std::streampos pos = ifs.tellg();
        bool found = false;
        int64_t oldTime = 0;
        std::string timeFormat = "%Y-%m-%dT%H:%M:%S";
        // Find the end of the second last set of stream metrics
        while (std::getline(ifs, line)) {
            std::string timestamp = Split(line, "|")[0];
            std::tm tm = {};
            std::stringstream ss(timestamp);
            ss >> std::get_time(&tm, timeFormat.c_str());
            std::chrono::system_clock::time_point point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
            int64_t time = std::chrono::duration_cast<std::chrono::seconds>(point.time_since_epoch()).count();
            if (time - oldTime >= PRINT_INTERVAL) {
                metric_start_pos = pos;
                found = true;
                oldTime = time;
            }
            pos = ifs.tellg();  // stores the last position
        }
        ASSERT_TRUE(found);
        ifs.clear();
        ifs.seekg(metric_start_pos);
        while (std::getline(ifs, line)) {
            if ((line.find(" exit") == std::string::npos)) {
                auto scMetrics = Split(line, "/");
                ASSERT_TRUE(scMetrics.size() > 0);
                auto streamName = scMetrics[0].substr(scMetrics[0].find_last_of("|") + 2);
                scMetrics.erase(scMetrics.begin());
                allScMetrics.emplace(streamName, scMetrics);
            }
        }
    }

    std::string GetScMetric(const std::vector<std::string> &scMetrics, StreamMetric metric)
    {
        // Subtract index by NumLocalProducers since it is the first sc metric, add one to account for stream name
        int index = (int)metric - (int)StreamMetric::NumLocalProducers;
        return scMetrics[index];
    }

    void VerifyStreamMetrics(const std::unordered_map<std::string, std::vector<std::string>> &scMetrics,
                             const std::unordered_map<std::string, std::vector<std::string>> &expected,
                             const std::vector<StreamMetric> &metricsToVerify)
    {
        ASSERT_EQ(scMetrics.size(), expected.size());
        for (auto &metric : expected) {
            // Verify stream name
            ASSERT_TRUE(scMetrics.count(metric.first) == 1);
            auto scMetric = scMetrics.at(metric.first);
            LOG(INFO) << "Verifying stream: " << metric.first;
            for (size_t i = 0; i < metricsToVerify.size(); i++) {
                LOG(INFO) << "Verifying metric: " << (int)metricsToVerify[i];
                ASSERT_EQ(GetScMetric(scMetric, metricsToVerify[i]), metric.second[i]);
            }
        }
    }

    void CreateOneWorkerMetricsScenario(std::shared_ptr<StreamClient> &client1,
                                        std::vector<std::shared_ptr<Producer>> &producers,
                                        std::vector<std::shared_ptr<Consumer>> &consumers,
                                        std::string streamName1, std::string streamName2)
    {
        // worker 0
        // stream1: 3 producer, 1 consumer
        // stream2: 2 producer, 2 consumer
        DS_ASSERT_OK(InitClient(0, client1));

        DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName1, 3 }, { streamName2, 2 } }, producers,
                                               { { streamName1, "sub1" }, { streamName2, "sub1" },
                                               { streamName2, "sub2" } }, consumers, false));
    }

    void CreateTwoWorkerMetricsScenario(std::shared_ptr<StreamClient> &client1, std::shared_ptr<StreamClient> &client2,
                                        std::vector<std::shared_ptr<Producer>> &producers,
                                        std::vector<std::shared_ptr<Consumer>> &consumers,
                                        std::string streamName1, std::string streamName2,
                                        std::string streamName3)
    {
        // worker 0
        // stream1: 3 producer, 2 consumer
        // stream2: 2 consumer
        // stream3: 2 producer
        // worker 1
        // stream2: 3 producer
        // stream3: 1 producer, 1 consumer
        DS_ASSERT_OK(InitClient(0, client1));
        DS_ASSERT_OK(InitClient(1, client2));

        DS_ASSERT_OK(CreateProducerAndConsumer(
            client1, { { streamName1, 3 }, { streamName3, 2 } }, producers,
            { { streamName1, "sub1" }, { streamName2, "sub1" }, { streamName1, "sub2" }, { streamName2, "sub2" } },
            consumers, true));
        DS_ASSERT_OK(CreateProducerAndConsumer(client2, { { streamName2, 3 }, { streamName3, 1 } }, producers,
                                               { { streamName3, "sub1" } }, consumers, true));
    }

    void SetUp()
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown()
    {
        ExternalClusterTest::TearDown();
    }

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const int WORKER_NUM = 2;
    const int VLOG_LEVEL = 2;
    const int PRINT_INTERVAL = 2;
    const int DEFAULT_WAIT_TIME = 1000;
    const int DEFAULT_NUM_ELEMENT = 20;
    const int TEST_ELEMENT_SIZE = 2 * KB - 128;
    const int TEST_ELEMENT2_SIZE = 4 * KB - 256;
    const int MAX_STREAM_SIZE = 2 * MB;
    const int CACHE_PAGES = 16;
    const int LOG_LEVEL = 2;
    const int STREAM_SIZE_MB = 2;
    const int SLEEP_TIME = 2;
    const int LONG_SLEEP_TIME = 5;
    const int RELEASE_PAGE_SLEEP_TIME = 15;
    const int BIG_ELEMENT_SIZE = 8 * KB;
    const int DELAY_FLUSH_TIME = 3000;
};

TEST_F(SCMetricsTest, NumLocalProducersConsumers)
{
    std::shared_ptr<StreamClient> client1;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumLocalProducers, StreamMetric::NumLocalConsumers };
    std::string streamName1 = "TestMetricsNumLocalProdCon_s1";
    std::string streamName2 = "TestMetricsNumLocalProdCon_s2";
    std::unordered_map<std::string, std::vector<std::string>> expected = { { streamName1, { "3", "1" } },
                                                                           { streamName2, { "2", "2" } } };

    CreateOneWorkerMetricsScenario(client1, producers, consumers, streamName1, streamName2);
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);
    // Close some
    const int prodCloseNum = 4;
    const int conCloseNum = 2;
    for (int i = 0; i < prodCloseNum; i++) {
        // 3 s1, 1 s2
        DS_ASSERT_OK(producers[i]->Close());
    }
    for (int i = 0; i < conCloseNum; i++) {
        // 1 s1, 1 s2
        DS_ASSERT_OK(consumers[i]->Close());
    }

    sleep(SLEEP_TIME);
    expected = {
        { streamName1, { "0", "0" } },
        { streamName2, { "1", "1" } },
    };

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    // Close rest and delete stream
    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
    DS_ASSERT_OK(client1->DeleteStream(streamName1));
    DS_ASSERT_OK(client1->DeleteStream(streamName2));
}

TEST_F(SCMetricsTest, NumRemoteProducersConsumers)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumLocalProducers, StreamMetric::NumRemoteProducers,
                                                  StreamMetric::NumLocalConsumers, StreamMetric::NumRemoteConsumers };
    std::string s1 = "testMetricsRemoteProdCon_s1";
    std::string s2 = "testMetricsRemoteProdCon_s2";
    std::string s3 = "testMetricsRemoteProdCon_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "3", "0", "2", "0" } },
                                                                            { s2, { "0", "1", "2", "0" } },
                                                                            { s3, { "2", "0", "0", "1" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "3", "0", "0", "2" } },
                                                                            { s3, { "1", "1", "1", "0" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    // Close some
    const int prodIndex = 8;  // s3
    const int conIndex = 4;   // s3
    DS_ASSERT_OK(producers[prodIndex]->Close());
    DS_ASSERT_OK(consumers[conIndex]->Close());
    sleep(SLEEP_TIME);
    expected1 = { { s1, { "3", "0", "2", "0" } },
                  { s2, { "0", "1", "2", "0" } },
                  { s3, { "2", "0", "0", "0" } } };
    expected2 = { { s2, { "3", "0", "0", "2" } }, { s3, { "0", "0", "0", "0" } } };
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);
    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, SharedMemoryUsed)
{
    std::shared_ptr<StreamClient> client1;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::SharedMemoryUsed };
    std::string streamName1 = "TestMetricsSharedMemUsed_s1";
    std::string streamName2 = "TestMetricsSharedMemUsed_s2";
    std::unordered_map<std::string, std::vector<std::string>> expected = {
        { streamName1, { std::to_string(3 * 40 * KB + 4 * 64) } }, { streamName2,
        { std::to_string(2 * 40 * KB + 4 * 64) } }
    };

    CreateOneWorkerMetricsScenario(client1, producers, consumers, streamName1, streamName2);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size()));
    }

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, LocalMemoryUsed)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::LocalMemoryUsed };
    std::string s1 = "testMetricsLocalMemUsed_s1";
    std::string s2 = "testMetricsLocalMemUsed_s2";
    std::string s3 = "testMetricsLocalMemUsed_s3";

    // Streams with remote producer will have local memory usage
    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "0" } },
                                                                            { s2, { std::to_string(40 * KB) } },
                                                                            { s3, { "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = {
        { s2, { "0" } }, { s3, { std::to_string(40 * KB) } }
    };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }
    sleep(SLEEP_TIME);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumTotalElementsSent)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumTotalElementsSent,
                                                  StreamMetric::NumTotalElementsReceived,
                                                  StreamMetric::NumTotalElementsAcked };
    std::string s1 = "testMetricsTotalEleSent_s1";
    std::string s2 = "testMetricsTotalEleSent_s2";
    std::string s3 = "testMetricsTotalEleSent_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "60", "0", "0" } },
                                                                            { s2, { "0", "0", "0" } },
                                                                            { s3, { "40", "0", "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "60", "0", "0" } },
                                                                            { s3, { "20", "0", "0" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumTotalElementsSentProducerClose)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumTotalElementsSent,
                                                  StreamMetric::NumTotalElementsReceived,
                                                  StreamMetric::NumTotalElementsAcked };
    std::string s1 = "testMetricsEleSentProdClose_s1";
    std::string s2 = "testMetricsEleSentProdClose_s2";
    std::string s3 = "testMetricsEleSentProdClose_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "60", "0", "0" } },
                                                                            { s2, { "0", "0", "0" } },
                                                                            { s3, { "40", "0", "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "60", "0", "0" } },
                                                                            { s3, { "20", "0", "0" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
        producer->Close();
    }
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);
}

TEST_F(SCMetricsTest, NumTotalElementsReceived)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumTotalElementsSent,
                                                  StreamMetric::NumTotalElementsReceived,
                                                  StreamMetric::NumTotalElementsAcked };
    std::string s1 = "testMetricsEleRecv_s1";
    std::string s2 = "testMetricsEleRecv_s2";
    std::string s3 = "testMetricsEleRecv_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "60", "60", "0" } },
                                                                            { s2, { "0", "60", "0" } },
                                                                            { s3, { "40", "0", "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "60", "0", "0" } },
                                                                            { s3, { "20", "60", "0" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size(), false));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumTotalEleRecvLateConsumer)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumTotalElementsSent,
                                                  StreamMetric::NumTotalElementsReceived,
                                                  StreamMetric::NumTotalElementsAcked };
    std::string s1 = "testMetricsRecvLateCon_s1";
    std::string s2 = "testMetricsRecvLateCon_s2";
    std::string s3 = "testMetricsRecvLateCon_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "60", "40", "40" } },
                                                                            { s2, { "0", "40", "40" } },
                                                                            { s3, { "40", "0", "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "60", "0", "0" } },
                                                                            { s3, { "20", "40", "40" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    int numProdToRecv = 2;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, numProdToRecv, true));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    // Create late consumers
    DS_ASSERT_OK(CreateProducerAndConsumer(client1, {}, producers, { { s1, "sub3" }, { s2, "sub3" } },
                                           consumers, true));
    numProdToRecv = 1;
    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, false, nullptr, numProdToRecv, false));
    }

    expected1 = { { s1, { "60", "60", "40" } },
                  { s2, { "0", "60", "40" } },
                  { s3, { "40", "0", "0" } } };

    expected2 = { { s2, { "60", "0", "0" } }, { s3, { "20", "60", "40" } } };

    sleep(SLEEP_TIME);
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumTotalElementsAcked)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumTotalElementsSent,
                                                  StreamMetric::NumTotalElementsReceived,
                                                  StreamMetric::NumTotalElementsAcked };
    std::string s1 = "testMetricsEleAcked_s1";
    std::string s2 = "testMetricsEleAcked_s2";
    std::string s3 = "testMetricsEleAcked_s3";

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "60", "60", "60" } },
                                                                            { s2, { "0", "60", "60" } },
                                                                            { s3, { "40", "0", "0" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "60", "0", "0" } },
                                                                            { s3, { "20", "60", "60" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size()));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumSendReceiveRequests)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumSendRequests, StreamMetric::NumReceiveRequests };
    std::string s1 = "testMetricsSendRecvReq_s1";
    std::string s2 = "testMetricsSendRecvReq_s2";
    std::string s3 = "testMetricsSendRecvReq_s3";
    // Each request executed twice, one sucess one failure
    // prodNum * eleNum * times, consNum * eleNum * times
    std::unordered_map<std::string, std::vector<std::string>> expected1 = {
        { s1, { "120", "4" } },  // 3 * 20 * 2, 2 * 2
        { s2, { "0", "4" } },    // 0, 2 * 2
        { s3, { "80", "0" } }    // 2 * 20 * 2, 0
    };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = {
        { s2, { "120", "0" } },  // 3 * 20 * 2
        { s3, { "40", "2" } }    // 20 * 2, 1 * 2
    };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
        DS_ASSERT_NOT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE, -1));
    }

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size()));
        DS_ASSERT_OK(ConsumeAll(consumer, 0, true, nullptr, producers.size(), false));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, LEVEL1_NumPages)
{
    std::shared_ptr<StreamClient> client1;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumPagesCreated, StreamMetric::NumPagesReleased,
                                                  StreamMetric::NumPagesInUse, StreamMetric::NumPagesCached };
    std::string streamName1 = "TestMetricsPages_s1";
    std::string streamName2 = "TestMetricsPages_s2";
    std::unordered_map<std::string, std::vector<std::string>> expected = { { streamName1, { "30", "0", "30", "0" } },
                                                                           { streamName2, { "20", "0", "20", "0" } } };

    CreateOneWorkerMetricsScenario(client1, producers, consumers, streamName1, streamName2);

    int i = 0;
    // Each producer sends 20 elements -> 40KB
    // Each element is slightly less than 2KB to account for page header overhead
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, TEST_ELEMENT_SIZE));
    }

    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    // One page in use still
    expected = { { streamName1, { "30", std::to_string(30 - CACHE_PAGES - 1), "1", std::to_string(CACHE_PAGES) } },
                 { streamName2, { "20", std::to_string(20 - CACHE_PAGES - 1), "1", std::to_string(CACHE_PAGES) } } };

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size()));
    }

    sleep(RELEASE_PAGE_SLEEP_TIME);  // wait until pages are cleaned up

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, LEVEL1_NumBigPages)
{
    std::shared_ptr<StreamClient> client1;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumPagesCreated, StreamMetric::NumPagesReleased,
                                                  StreamMetric::NumPagesCached, StreamMetric::NumBigPagesCreated,
                                                  StreamMetric::NumBigPagesReleased };
    std::string streamName1 = "TestMetricsBigPages_s1";
    std::string streamName2 = "TestMetricsBigPages_s2";
    std::unordered_map<std::string, std::vector<std::string>> expected = {
        // all big pages, except 1 normal page from CreatePageZero()
        { streamName1, { "1", "0", "0", "60", "0" } },
        { streamName2, { "1", "0", "0", "40", "0" } }
    };

    CreateOneWorkerMetricsScenario(client1, producers, consumers, streamName1, streamName2);
    int i = 0;
    // Each producer sends 20 8KB elements -> BigElement since pageSize is 4KB
    for (auto &producer : producers) {
        i++;
        DS_ASSERT_OK(Produce(producer, "producer" + std::to_string(i), DEFAULT_NUM_ELEMENT, BIG_ELEMENT_SIZE));
    }
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    for (auto &consumer : consumers) {
        DS_ASSERT_OK(ConsumeAll(consumer, DEFAULT_WAIT_TIME, true, nullptr, producers.size()));
    }
    // all big pages, except 1 normal page from CreatePageZero()
    expected = { { streamName1, { "1", "0", "0", "60", "60" } }, { streamName2, { "1", "0", "0", "40", "40" } } };
    sleep(RELEASE_PAGE_SLEEP_TIME);  // wait until pages are cleaned up
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumLocalProducersBlocked)
{
    std::shared_ptr<StreamClient> client1;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::string streamName1 = "testMetricsLocalProdBlocked_s1";
    std::string streamName2 = "testMetricsLocalProdBlocked_s2";
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumLocalProducersBlocked };

    std::unordered_map<std::string, std::vector<std::string>> expected = { { streamName1, { "1" } },
                                                                           { streamName2, { "0" } } };

    DS_ASSERT_OK(InitClient(0, client1));

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName1, 3 }, { streamName2, 2 } }, producers,
                                           { { streamName1, "sub1" }, { streamName2, "sub1" },
                                           { streamName2, "sub2" } }, consumers, true));
    const int eleNum = 550;
    const int waitTime = 30000;
    std::thread sendThread([&]() { Produce(producers[0], "producer", eleNum, TEST_ELEMENT2_SIZE, waitTime); });

    sleep(LONG_SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    const int expectNum = 100;
    const int numRecvCons = 1;
    for (int i = 0; i < numRecvCons; i++) {
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumers[i]->Receive(expectNum, DEFAULT_WAIT_TIME, outElements));
        DS_ASSERT_OK(consumers[i]->Ack(outElements.back().id));
    }

    sendThread.join();

    expected = { { streamName1, { "0" } }, { streamName2, { "0" } } };
    sleep(SLEEP_TIME);
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumRemoteProducersConsumersBlocked)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::string streamName1 = "testMetricsRemoteProConBlocked_s1";
    std::string streamName2 = "testMetricsRemoteProConBlocked_s2";
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumRemoteProducersBlocked,
                                                  StreamMetric::NumRemoteConsumersBlocking };

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { streamName1, { "0", "2" } },
                                                                            { streamName2, { "", "" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { streamName1, { "1", "0" } },
                                                                            { streamName2, { "0", "0" } } };

    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));

    DS_ASSERT_OK(CreateProducerAndConsumer(client1, { { streamName1, 1 } }, producers, {}, consumers, true));
    DS_ASSERT_OK(CreateProducerAndConsumer(client2, { { streamName1, 1 }, { streamName2, 1 } }, producers,
                                           { { streamName1, "sub1" }, { streamName1, "sub2" },
                                           { streamName2, "sub1" } }, consumers, true));

    const int waitTime = 30000;
    // First send 200 elements to use up some memory in worker2
    Produce(producers[1], "producer2", 200, TEST_ELEMENT2_SIZE, waitTime);

    // Send 400 elements to worker1, so worker2 will block worker1 producer
    int eleNum = 400;
    Produce(producers[0], "producer1", eleNum, TEST_ELEMENT2_SIZE, waitTime);

    sleep(LONG_SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    const int expectNum = 100;
    const int numRecvCons = 2;
    for (int i = 0; i < numRecvCons; i++) {
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumers[i]->Receive(expectNum, DEFAULT_WAIT_TIME, outElements));
        DS_ASSERT_OK(consumers[i]->Ack(outElements.back().id));
    }

    expected1 = { { streamName1, { "0", "0" } }, { streamName2, { "", "" } } };

    expected2 = { { streamName1, { "0", "0" } }, { streamName2, { "0", "0" } } };
    sleep(SLEEP_TIME);
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, RetainDataState)
{
    std::shared_ptr<StreamClient> client1;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::string streamName = "testMetricsRetainDataState";
    std::vector<StreamMetric> metricsToVerify = {
        StreamMetric::RetainDataState,
    };

    // Streams with remote producer will have local memory usage
    std::unordered_map<std::string, std::vector<std::string>> expected = {
        { streamName, { std::to_string(RetainDataState::RETAIN) } }
    };

    DS_ASSERT_OK(InitClient(0, client1));

    ProducerConf conf;
    conf.delayFlushTime = DELAY_FLUSH_TIME;
    conf.pageSize = PAGE_SIZE;  // 4K
    conf.maxStreamSize = MAX_STREAM_SIZE;
    conf.autoCleanup = true;
    conf.retainForNumConsumers = 1;  // retain data until one consumer

    std::shared_ptr<Producer> producer;
    // Create producer and send data
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    for (int i = 0; i < DEFAULT_NUM_ELEMENT; i++) {
        DS_ASSERT_OK(producer->Send(element));
    }
    sleep(SLEEP_TIME);
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);

    // Create a late consumer
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    std::vector<Element> outElements;
    // Now should get the data
    DS_ASSERT_OK(consumer->Receive(DEFAULT_NUM_ELEMENT, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(DEFAULT_NUM_ELEMENT));

    expected = { { streamName, { std::to_string(RetainDataState::NOT_RETAIN) } } };

    sleep(SLEEP_TIME);
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);
}

TEST_F(SCMetricsTest, NumProducersConsumersMaster)
{
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::unordered_map<std::string, std::vector<std::string>> sc1Metrics;
    std::string s1 = "TestMetricsProdConMaster_s1";
    std::string s2 = "TestMetricsProdConMaster_s2";
    std::string s3 = "TestMetricsProdConMaster_s3";
    // StreamMetric::NumProducersMaster: Number of worker that have at least 1 producer for the stream.
    // If there are 2 or more local producer for the same stream on the same worker, that count as 1.
    std::vector<StreamMetric> metricsToVerify = {
        StreamMetric::NumProducersMaster,
        StreamMetric::NumConsumersMaster,
    };

    std::unordered_map<std::string, std::vector<std::string>> expected1 = { { s1, { "1", "2" } },
                                                                            { s2, { "1", "2" } },
                                                                            { s3, { "2", "1" } } };

    std::unordered_map<std::string, std::vector<std::string>> expected2 = { { s2, { "", "" } },
                                                                            { s3, { "", "" } } };

    CreateTwoWorkerMetricsScenario(client1, client2, producers, consumers, s1, s2, s3);
    sleep(SLEEP_TIME);

    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);

    // Close some
    const int prodIndex = 8;  // s3
    const int conIndex = 4;   // s3
    DS_ASSERT_OK(producers[prodIndex]->Close());
    DS_ASSERT_OK(consumers[conIndex]->Close());
    sleep(SLEEP_TIME);
    expected1 = { { s1, { "1", "2" } }, { s2, { "1", "2" } }, { s3, { "1", "0" } } };
    expected2 = { { s2, { "", "" } }, { s3, { "", "" } } };
    GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
    VerifyStreamMetrics(sc0Metrics, expected1, metricsToVerify);
    GetStreamMetrics(1, "sc_metrics.log", sc1Metrics);
    VerifyStreamMetrics(sc1Metrics, expected2, metricsToVerify);
    DS_ASSERT_OK(CloseAllProducerAndConsumer(producers, consumers));
}

TEST_F(SCMetricsTest, NumPagesInit)
{
    std::shared_ptr<StreamClient> client1;
    std::unordered_map<std::string, std::vector<std::string>> sc0Metrics;
    std::vector<StreamMetric> metricsToVerify = { StreamMetric::NumPagesCreated, StreamMetric::NumPagesReleased,
                                                  StreamMetric::NumPagesCached, StreamMetric::NumBigPagesCreated,
                                                  StreamMetric::NumBigPagesReleased };
    std::string streamName = "testMetricsNumPgsInit";
    std::unordered_map<std::string, std::vector<std::string>> expected = { { streamName, { "1", "0", "0", "0", "0" } }};

    DS_ASSERT_OK(InitClient(0, client1));

    ProducerConf conf;
    conf.delayFlushTime = DELAY_FLUSH_TIME;
    conf.pageSize = PAGE_SIZE;  // 4K
    conf.maxStreamSize = MAX_STREAM_SIZE;
    const int numIterations = 3;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    // Verify values are properly init
    for (int i = 0; i < numIterations; i++) {
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer, true));
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
        sleep(SLEEP_TIME);
        GetStreamMetrics(0, "sc_metrics.log", sc0Metrics);
        VerifyStreamMetrics(sc0Metrics, expected, metricsToVerify);
        producer->Close();
        consumer->Close();
        client1->DeleteStream(streamName);
    }
}
}  // namespace st
}  // namespace datasystem
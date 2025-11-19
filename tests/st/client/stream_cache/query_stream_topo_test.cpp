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
#include <random>
#include <thread>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/random_data.h"
#include "sc_client_common.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class QueryStreamTopoTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 5;
        opts.numMasters = 1;
        opts.enableDistributedMaster = "false";
        opts.numRpcThreads = 0;
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
        clientVector_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTest()
    {
        workerAddressVector_.resize(clientNum_);
        for (int i = 0; i < clientNum_; ++i) {
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddressVector_[i]));
            LOG(INFO) << FormatString("Worker%d: <%s>", i, workerAddressVector_[i].ToString());
        }

        clientVector_.resize(clientNum_);
        for (size_t i = 0; i < clientVector_.size(); i++) {
            InitStreamClient(i, clientVector_[i]);
        }
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    Status ProduceRandomData(std::shared_ptr<Producer> &producer, const std::string &producerName, uint64_t eleSz,
                             uint64_t eleNum)
    {
        ElementGenerator elementGenerator(eleSz);
        auto strs = elementGenerator.GenElements(producerName, eleNum, 1);
        for (size_t i = 0; i < eleNum; i++) {
            RETURN_IF_NOT_OK(producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
        }
        return producer->Close();
    }

    Status ProduceMockData(std::shared_ptr<Producer> &producer, size_t round, int timeoutMs = 0)
    {
        std::vector<std::string> strs;
        strs.emplace_back("hello world");
        strs.emplace_back("hello China");
        strs.emplace_back("hello 2022");

        std::stringstream ss;
        ss << FormatString("\n============= Round:%d Send Data =============\n", round);
        for (size_t i = 0; i < strs.size(); i++) {
            if (timeoutMs != 0 && i == strs.size() - 1) {
                LOG(INFO) << FormatString("Round:%d, after number %d element flush, sleep for %d ms", round, i,
                                          timeoutMs);
                std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
            }
            ss << FormatString("String %d is:%s\n", i, strs[i]);
            RETURN_IF_NOT_OK(producer->Send(Element((uint8_t *)strs[i].data(),  strs[i].size())));
        }
        ss << "=============================================\n";
        LOG(INFO) << ss.str();
        return producer->Close();
    }

    void FinanceCase(size_t round, bool withRandomData = true, bool withRandomNode = false);

    void TimeoutCase(size_t round, int timeoutMs = 0);

    std::vector<std::shared_ptr<StreamClient>> clientVector_;
    std::vector<HostPort> workerAddressVector_;
    uint8_t clientNum_ = 5;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

class NewRandom : RandomData {
public:
    std::vector<uint64_t> RandomSequenceFromSet(std::set<uint64_t> &set)
    {
        std::vector<uint64_t> sequence;
        auto upperLimit = *std::max_element(set.begin(), set.end());
        auto lowerLimit = *std::min_element(set.begin(), set.end());
        thread_local static std::uniform_int_distribution<uint64_t> distribution(lowerLimit, upperLimit);
        while (!set.empty()) {
            thread_local static auto generator = randomDevice_;
            auto ele = distribution(generator);
            if (set.erase(ele) == 1) {
                sequence.emplace_back(ele);
            }
        }
        return sequence;
    }
};

TEST_F(QueryStreamTopoTest, QueryTest)
{
    std::string stream1("testQueryTest");

    uint64_t producersCount = 0;
    uint64_t consumersCount = 0;

    // stream not exists.
    DS_ASSERT_OK(clientVector_[0]->QueryGlobalProducersNum(stream1, producersCount));
    ASSERT_EQ(producersCount, 0ul);
    DS_ASSERT_OK(clientVector_[0]->QueryGlobalConsumersNum(stream1, consumersCount));
    ASSERT_EQ(consumersCount, 0ul);

    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    std::shared_ptr<Producer> node1Producer2;
    DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, node1Producer2, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(clientVector_[0]->Subscribe(stream1, config1, node1Consumer1));

    DS_ASSERT_OK(clientVector_[0]->QueryGlobalProducersNum(stream1, producersCount));
    // Producer count will still be 1
    // as master just counts number of workers having atleast one producer
    ASSERT_EQ(producersCount, 1ul);
    DS_ASSERT_OK(clientVector_[0]->QueryGlobalConsumersNum(stream1, consumersCount));
    ASSERT_EQ(consumersCount, size_t(1));

    producersCount = 0;
    consumersCount = 100;
    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(clientVector_[1]->Subscribe(stream1, config2, node2Consumer1));

    DS_ASSERT_OK(clientVector_[1]->QueryGlobalProducersNum(stream1, producersCount));
    DS_ASSERT_OK(clientVector_[1]->QueryGlobalConsumersNum(stream1, consumersCount));
    ASSERT_EQ(consumersCount, size_t(2));
}

TEST_F(QueryStreamTopoTest, ConcurrentQueryTest)
{
    std::string stream1("testConcurrentQueryTest");
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub0", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub1", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub2", SubscriptionType::STREAM) };
    ThreadPool pool(clientNum_);
    pool.Submit([this, stream1, &configVector]() {
        thread_local uint32_t queryRet = 0;
        std::shared_ptr<Producer> n0p0;
        std::shared_ptr<Consumer> n0c0;
        DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p0, defaultProducerConf_));
        DS_ASSERT_OK(clientVector_[0]->Subscribe(stream1, configVector[0], n0c0));

        std::string localHostName = workerAddressVector_[0].ToString();
        uint64_t producerNum = 0;
        uint64_t consumerNum = 0;
        DS_ASSERT_OK(clientVector_[0]->QueryGlobalProducersNum(stream1, producerNum));
        DS_ASSERT_OK(clientVector_[0]->QueryGlobalConsumersNum(stream1, consumerNum));

        producerNum = 0;
        consumerNum = 0;
        DS_ASSERT_OK(clientVector_[0]->QueryGlobalProducersNum(stream1, producerNum));
        ASSERT_GE(producerNum, size_t(1));
        ASSERT_LE(producerNum, size_t(3));
        LOG(INFO) << FormatString("Thread 0, #<%d>, global pub node number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);
        consumerNum = 0;
        DS_ASSERT_OK(clientVector_[0]->QueryGlobalConsumersNum(stream1, consumerNum));
        ASSERT_GE(consumerNum, size_t(1));
        ASSERT_LE(consumerNum, size_t(3));
        LOG(INFO) << FormatString("Thread 0, #<%d>, global consumer number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);

        DS_ASSERT_OK(n0p0->Close());
        DS_ASSERT_OK(n0c0->Close());
        clientVector_[0]->DeleteStream(stream1);
    });
    pool.Submit([this, stream1, &configVector]() {
        thread_local uint32_t queryRet = 0;
        std::shared_ptr<Producer> n1p0;
        std::shared_ptr<Consumer> n1c0;
        DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p0, defaultProducerConf_));
        DS_ASSERT_OK(clientVector_[1]->Subscribe(stream1, configVector[1], n1c0));

        std::string localHostName = workerAddressVector_[1].ToString();
        uint64_t producerNum = 0;
        uint64_t consumerNum = 0;
        DS_ASSERT_OK(clientVector_[1]->QueryGlobalProducersNum(stream1, producerNum));
        DS_ASSERT_OK(clientVector_[1]->QueryGlobalConsumersNum(stream1, consumerNum));

        producerNum = 0;
        consumerNum = 0;
        DS_ASSERT_OK(clientVector_[1]->QueryGlobalProducersNum(stream1, producerNum));
        ASSERT_GE(producerNum, size_t(1));
        ASSERT_LE(producerNum, size_t(3));
        consumerNum = 0;
        LOG(INFO) << FormatString("Thread 1, #<%d>, global pub node number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);
        DS_ASSERT_OK(clientVector_[1]->QueryGlobalConsumersNum(stream1, consumerNum));
        ASSERT_GE(consumerNum, size_t(1));
        ASSERT_LE(consumerNum, size_t(3));
        LOG(INFO) << FormatString("Thread 1, #<%d>, global consumer number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);

        DS_ASSERT_OK(n1p0->Close());
        DS_ASSERT_OK(n1c0->Close());
        clientVector_[1]->DeleteStream(stream1);
    });
    pool.Submit([this, stream1, &configVector]() {
        thread_local uint32_t queryRet = 0;
        std::shared_ptr<Producer> n2p0;
        std::shared_ptr<Consumer> n2c0;
        DS_ASSERT_OK(clientVector_[2]->CreateProducer(stream1, n2p0, defaultProducerConf_));
        DS_ASSERT_OK(clientVector_[2]->Subscribe(stream1, configVector[2], n2c0));

        uint64_t producerNum = 0;
        uint64_t consumerNum = 0;
        DS_ASSERT_OK(clientVector_[2]->QueryGlobalProducersNum(stream1, producerNum));
        DS_ASSERT_OK(clientVector_[2]->QueryGlobalConsumersNum(stream1, consumerNum));

        LOG(INFO) << FormatString("Thread 2, #<%d>, global pub node number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);
        ASSERT_GE(consumerNum, size_t(1));
        ASSERT_LE(consumerNum, size_t(3));
        LOG(INFO) << FormatString("Thread 2, #<%d>, global consumer number query ret:<%d>",
                                  std::hash<std::thread::id>{}(std::this_thread::get_id()), queryRet);

        DS_ASSERT_OK(n2p0->Close());
        DS_ASSERT_OK(n2c0->Close());
        clientVector_[2]->DeleteStream(stream1);
    });
}

void QueryStreamTopoTest::FinanceCase(size_t round, bool withRandomData, bool withRandomNode)
{
    size_t nodeNum = 3;
    std::string stream1 = FormatString("FinanceCase%d-S1", round);
    std::string stream2 = FormatString("FinanceCase%d-S2", round);
    LOG(INFO) << FormatString("Src ----> Process stream name:%s", stream1);
    LOG(INFO) << FormatString("Process ----> Sink stream name:%s", stream2);

    ThreadPool pool(nodeNum);
    std::vector<std::future<Status>> futs;
    size_t eleSz = 16;
    uint64_t eleNum = 3;

    std::vector<size_t> nodeIdx;
    std::set<size_t> idxSet = { 0, 1, 2 };
    if (withRandomNode) {
        auto rndGenerator = NewRandom();
        nodeIdx = rndGenerator.RandomSequenceFromSet(idxSet);
    } else {
        nodeIdx = { 0, 1, 2 };
    }

    // w1:p1(stream1) and then continuously query, then producer data
    futs.emplace_back(pool.Submit([this, &stream1, eleSz, eleNum, round, withRandomData, &nodeIdx]() {
        std::shared_ptr<Producer> producer0;
        RETURN_IF_NOT_OK(clientVector_[nodeIdx[0]]->CreateProducer(stream1, producer0, defaultProducerConf_));
        thread_local auto begin = Timer();
        while (begin.ElapsedMilliSecond() <= 10 * 1000) {
            // Simulate finance scenario, detect downstream consumer register successfully
            uint64_t consumerNum = 0;
            RETURN_IF_NOT_OK(clientVector_[nodeIdx[0]]->QueryGlobalConsumersNum(stream1, consumerNum));
            if (consumerNum > 0) {
                LOG(INFO) << FormatString("[S:%s] Node 1 detect %d consumer subscribe success, start to producer data",
                                          stream1, consumerNum);
                break;
            }
        }
        // Send.
        if (withRandomData) {
            RETURN_IF_NOT_OK(ProduceRandomData(producer0, "producer1", eleSz, eleNum));
        } else {
            RETURN_IF_NOT_OK(ProduceMockData(producer0, round));
        }
        return Status::OK();
    }));
    // w2:c1 (stream1, sub1)
    futs.emplace_back(pool.Submit([this, &stream1, &stream2, eleNum, round, withRandomData, &nodeIdx]() {
        std::shared_ptr<Consumer> consumer1 = nullptr;
        SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(clientVector_[nodeIdx[1]]->Subscribe(stream1, config1, consumer1));
        CHECK_FAIL_RETURN_STATUS(consumer1, StatusCode::K_RUNTIME_ERROR, "Fail to subscribe");
        std::vector<Element> outElements;
        thread_local auto begin = Timer();
        uint64_t timeOut = 3 * 1000;
        uint64_t retry = 0;
        while (outElements.size() < eleNum && begin.ElapsedMilliSecond() <= timeOut) {
            std::vector<Element> output;
            consumer1->Receive(3, 0, output);
            outElements.insert(outElements.end(), output.begin(), output.end());
            retry++;
        }
        LOG(INFO) << FormatString("Round:%d, Process receive retry time:%d", round, retry);
        CHECK_FAIL_RETURN_STATUS(outElements.size() == eleNum, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Round:%d, Expect receive %d elements, Actually got %d elements", round,
                                              eleNum, outElements.size()));

        std::shared_ptr<Producer> producer1;
        RETURN_IF_NOT_OK(clientVector_[nodeIdx[1]]->CreateProducer(stream2, producer1, defaultProducerConf_));
        thread_local auto begin1 = Timer();
        while (begin1.ElapsedMilliSecond() <= timeOut) {
            // Simulate finance scenario, detect downstream consumer register successfully
            uint64_t consumerNum = 0;
            RETURN_IF_NOT_OK(clientVector_[nodeIdx[1]]->QueryGlobalConsumersNum(stream2, consumerNum));
            if (consumerNum > 0) {
                LOG(INFO) << FormatString("[S:%s] Node2 detect %d consumer subscribe success, start to producer data",
                                          stream2, consumerNum);
                break;
            }
        }
        if (!withRandomData) {
            std::stringstream ss;
            ss << FormatString("\n============= Round:%d Middle Data =============\n", round);
            size_t idx = 0;
            for (auto &ele : outElements) {
                std::string tmpString{ reinterpret_cast<const char *>(ele.ptr), ele.size };
                ss << FormatString("String %d is:%s\n", idx, tmpString);
                idx++;
            }
            ss << "===============================================\n";
            LOG(INFO) << ss.str();
        }
        for (auto &ele : outElements) {
            RETURN_IF_NOT_OK(producer1->Send(ele));
        }
        RETURN_IF_NOT_OK(producer1->Close());
        return consumer1->Close();
    }));
    futs.emplace_back(pool.Submit([this, &stream2, eleNum, round, withRandomData, &nodeIdx]() {
        std::shared_ptr<Consumer> consumer2 = nullptr;
        SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(clientVector_[nodeIdx[2]]->Subscribe(stream2, config2, consumer2));
        CHECK_FAIL_RETURN_STATUS(consumer2, StatusCode::K_RUNTIME_ERROR, "Fail to subscribe");
        std::vector<Element> outElements;
        thread_local auto begin = Timer();
        uint64_t retry = 0;
        uint64_t timeOut = 10 * 1000;
        while (outElements.size() < eleNum && begin.ElapsedMilliSecond() <= timeOut) {
            std::vector<Element> output;
            consumer2->Receive(3, 0, output);
            outElements.insert(outElements.end(), output.begin(), output.end());
            retry++;
        }
        LOG(INFO) << FormatString("Round:%d, Src Receive retry time:%d", round, retry);

        if (!withRandomData) {
            std::stringstream ss;
            ss << FormatString("\n============= Round:%d Recv Data =============\n", round);
            size_t idx = 0;
            for (auto &ele : outElements) {
                std::string tmpString{ reinterpret_cast<const char *>(ele.ptr), ele.size };
                ss << FormatString("String %d is:%s\n", idx, tmpString);
                idx++;
            }
            ss << "=============================================\n";
            LOG(INFO) << ss.str();
        }

        CHECK_FAIL_RETURN_STATUS(outElements.size() == eleNum, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Round:%d, Expect to receive %d elements, Actually got %d elements",
                                              round, eleNum, outElements.size()));
        return consumer2->Close();
    }));
    for (auto &fut : futs) {
        ASSERT_EQ(fut.get(), Status::OK());
    }
}

void QueryStreamTopoTest::TimeoutCase(size_t round, int timeoutMs)
{
    std::string stream1 = FormatString("TimeoutCase%d-S1", round);
    size_t nodeNum = 2;
    size_t eleNum = 3;
    std::vector<std::future<Status>> futs;
    ThreadPool pool(nodeNum);
    futs.emplace_back(pool.Submit([this, &stream1, round, timeoutMs]() {
        std::shared_ptr<Producer> producer0;
        RETURN_IF_NOT_OK(clientVector_[0]->CreateProducer(stream1, producer0, defaultProducerConf_));
        thread_local auto begin = Timer();
        while (begin.ElapsedMilliSecond() <= 10 * 1000) {
            // Simulate finance scenario, detect downstream consumer register successfully
            uint64_t consumerNum = 0;
            RETURN_IF_NOT_OK(clientVector_[0]->QueryGlobalConsumersNum(stream1, consumerNum));
            if (consumerNum > 0) {
                LOG(INFO) << FormatString("[S:%s] Node 1 detect %d consumer subscribe success, start to producer data",
                                          stream1, consumerNum);
                break;
            }
        }
        // Send.
        RETURN_IF_NOT_OK(ProduceMockData(producer0, round, timeoutMs));
        return Status::OK();
    }));
    futs.emplace_back(pool.Submit([this, &stream1, round, eleNum]() {
        std::shared_ptr<Consumer> consumer = nullptr;
        SubscriptionConfig config("sub0", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(clientVector_[1]->Subscribe(stream1, config, consumer));
        CHECK_FAIL_RETURN_STATUS(consumer, StatusCode::K_RUNTIME_ERROR, "Fail to subscribe");
        std::vector<Element> outElements;
        thread_local auto begin = Timer();
        uint64_t retry = 0;
        // Set receive loop timeout = 100 seconds
        uint64_t timeOut = 100 * 1000;
        while (outElements.size() < eleNum && begin.ElapsedMilliSecond() <= timeOut) {
            std::vector<Element> output;
            consumer->Receive(3, 0, output);
            outElements.insert(outElements.end(), output.begin(), output.end());
            retry++;
        }
        LOG(INFO) << FormatString("Round:%d, Src Receive retry time:%d", round, retry);
        std::stringstream ss;
        ss << FormatString("\n============= Round:%d Recv Data =============\n", round);
        size_t idx = 0;
        for (auto &ele : outElements) {
            std::string tmpString{ reinterpret_cast<const char *>(ele.ptr), ele.size };
            ss << FormatString("String %d is:%s\n", idx, tmpString);
            idx++;
        }
        ss << "=============================================\n";
        LOG(INFO) << ss.str();

        CHECK_FAIL_RETURN_STATUS(outElements.size() == eleNum, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Round:%d, Expect to receive %d elements, Actually got %d elements",
                                              round, eleNum, outElements.size()));
        return consumer->Close();
    }));
}

TEST_F(QueryStreamTopoTest, FinanceCaseFixedNode)
{
    auto rounds = 8;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { FinanceCase(round, false); });
    }
}

class QueryStreamTopoTest1 : public QueryStreamTopoTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        QueryStreamTopoTest::SetClusterSetupOptions(opts);
    }
};

TEST_F(QueryStreamTopoTest1, FinanceCaseRndNode)
{
    auto rounds = 8;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { FinanceCase(round, false, true); });
    }
}

TEST_F(QueryStreamTopoTest, DISABLED_TimeoutCase)
{
    const int timeoutMs = 80 * 1000;
    TimeoutCase(0, timeoutMs);
}

TEST_F(QueryStreamTopoTest, ConcurrentWithSubscribe)
{
    FinanceCase(0);
}

TEST_F(QueryStreamTopoTest, QueryDistributionTest)
{
    std::string stream1("stream1");
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub1", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub2", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub3", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub4", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub5", SubscriptionType::STREAM) };

    std::vector<std::shared_ptr<Producer>> producerVector(5);
    std::vector<std::shared_ptr<Consumer>> consumerVector(5);
    uint64_t producerNum = 0;
    uint64_t consumerNum = 0;
    for (int i = 0; i < 5; ++i) {
        DS_ASSERT_OK(clientVector_[i]->CreateProducer(stream1, producerVector[i], defaultProducerConf_));
        DS_ASSERT_OK(clientVector_[i]->Subscribe(stream1, configVector[i], consumerVector[i]));
    }
    for (int i = 0; i < 5; ++i) {
        producerNum = 0;
        DS_ASSERT_OK(clientVector_[i]->QueryGlobalProducersNum(stream1, producerNum));
        ASSERT_EQ(producerNum, size_t(5));

        consumerNum = 0;
        DS_ASSERT_OK(clientVector_[i]->QueryGlobalConsumersNum(stream1, consumerNum));
        ASSERT_EQ(consumerNum, size_t(5));
    }
}
}  // namespace st
}  // namespace datasystem

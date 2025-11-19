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
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/common/inject/inject_point.h"

using namespace datasystem::client::stream_cache;
using ::testing::Values;
namespace datasystem {
namespace st {
struct InputStreamInfo {
    InputStreamInfo &SetProducers(int producerNum)
    {
        this->producerNum = producerNum;
        return *this;
    }
    InputStreamInfo &AddSub(const std::string &subName, SubscriptionType type, int consumerNum)
    {
        subscriptions[subName] = std::make_pair(type, consumerNum);
        return *this;
    }
    int producerNum = 0;
    std::unordered_map<std::string, std::pair<SubscriptionType, int>> subscriptions;
};

struct StreamParas {
    explicit StreamParas(int pageSize = -1, int sharedMemMB = 1024, int regularSocketNum = 16, int streamSocketNum = 16)
        : pageSize(pageSize),
          sharedMemorySizeMB(sharedMemMB),
          regularSocketNum(regularSocketNum),
          streamSocketNum(streamSocketNum){};
    ~StreamParas() = default;
    InputStreamInfo &MutableStream(const std::string &streamName)
    {
        return this->streams[streamName];
    }
    int pageSize;
    int sharedMemorySizeMB;
    int regularSocketNum;
    int streamSocketNum;
    std::unordered_map<std::string, InputStreamInfo> streams;
};

struct OutputStreamInfo {
    std::atomic<uint64_t> totalSend;
    std::atomic<uint64_t> totalRecv;
    std::vector<std::shared_ptr<Producer>> producers;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Consumer>>> consumers;
};


// Constant expressions
constexpr int K_3 = 3, K_5 = 5, K_10 = 10, K_50 = 50, K_100 = 100;

class MultiProducerMultiConsumerTest : public SCClientCommon {
public:
    // Called by Base class SetUp
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        if (inputParas_.pageSize > 0) {
            opts.workerGflagParams = " -page_size=" + std::to_string(inputParas_.pageSize);
        }
        opts.workerGflagParams += " -shared_memory_size_mb=" + std::to_string(inputParas_.sharedMemorySizeMB);
        opts.numScRegularSocket = inputParas_.regularSocketNum;
        opts.numScStreamSocket = inputParas_.streamSocketNum;
        opts.numRpcThreads = 0;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitClient();
    }

    void TearDown() override
    {
        client_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    std::shared_ptr<StreamParas> GetInputParas(int streamNum, int producerNum, int subNum);
    Status AsyncCreateProducersAndConsumers(std::unordered_map<std::string, InputStreamInfo> &input,
                                            std::unordered_map<std::string, OutputStreamInfo> &output);
    Status MultiSendRecv(std::unordered_map<std::string, OutputStreamInfo> &output, int elementNum, int elementSize,
                         int flushIntervals, int elementBatchNum, int blockingMs);
    Status MultiDeleteStream(std::unordered_map<std::string, OutputStreamInfo> &output);
    std::vector<Element> GenerateElements(int elementNum, uint64_t elementSize, std::string &outData);

protected:
    void InitClient()
    {
        InitStreamClient(0, client_);
    }
    std::shared_ptr<StreamClient> client_ = nullptr;
    StreamParas inputParas_;
    std::unordered_map<std::string, OutputStreamInfo> streamInfo_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

std::shared_ptr<StreamParas> MultiProducerMultiConsumerTest::GetInputParas(int streamNum, int producerNum, int subNum)
{
    auto paras = std::make_shared<StreamParas>();
    for (int i = 0; i < streamNum; i++) {
        std::string streamName = "stream" + std::to_string(i);
        paras->MutableStream(streamName).SetProducers(producerNum);
        for (int j = 0; j < subNum; j++) {
            std::string subName = "sub" + std::to_string(j);
            paras->MutableStream(streamName).AddSub(subName, SubscriptionType::STREAM, 1);
        }
    }
    return paras;
}

Status MultiProducerMultiConsumerTest::AsyncCreateProducersAndConsumers(
    std::unordered_map<std::string, InputStreamInfo> &input, std::unordered_map<std::string, OutputStreamInfo> &output)
{
    output.reserve(input.size());
    std::vector<std::thread> producerThreads;
    std::vector<std::thread> consumerThreads;
    // Avoid data race when output[streamName] insert node and producer/consumer threads read output[streamName]
    for (auto &iter : input) {
        const auto &streamName = iter.first;
        auto &info = iter.second;
        // Create producers for the stream
        output[streamName].producers.resize(info.producerNum);
    }

    for (auto &iter : input) {
        const auto &streamName = iter.first;
        auto &info = iter.second;
        for (int i = 0; i < info.producerNum; i++) {
            producerThreads.emplace_back([i, streamName, &output, this]() {
                LOG(INFO) << "Start create producer " << i << " for stream " << streamName;
                std::shared_ptr<Producer> producer;
                EXPECT_EQ(client_->CreateProducer(streamName, producer), Status::OK());
                LOG(INFO) << FormatString("Finished create producer %d for stream %s", i, streamName);
                output[streamName].producers[i] = std::move(producer);
            });
        }
        // Create consumers for the subscriptions
        for (auto &subInfo : info.subscriptions) {
            const auto &subName = subInfo.first;
            SubscriptionConfig config(subName, subInfo.second.first);
            int consumerNum = subInfo.second.second;
            output[streamName].consumers[subName].resize(consumerNum);
            for (int i = 0; i < consumerNum; i++) {
                consumerThreads.emplace_back([i, streamName, subName, config, &output, this]() {
                    LOG(INFO) << FormatString("Start create consumer %d for (stream %s, sub %s).", i, streamName,
                                              subName);
                    std::shared_ptr<Consumer> consumer;
                    EXPECT_EQ(client_->Subscribe(streamName, config, consumer), Status::OK());
                    LOG(INFO) << FormatString("Finished create consumer %d for (stream %s, sub %s).", i, streamName,
                                              subName);
                    output[streamName].consumers[subName][i] = std::move(consumer);
                });
            }
        }
    }
    for (std::thread &producer : producerThreads) {
        producer.join();
    }
    for (std::thread &consumer : consumerThreads) {
        consumer.join();
    }
    return Status::OK();
}

Status MultiProducerMultiConsumerTest::MultiSendRecv(std::unordered_map<std::string, OutputStreamInfo> &output,
                                                     int elementNum, int elementSize, int flushIntervals,
                                                     int elementBatchNum, int blockingMs)
{
    std::string data;
    std::vector<Element> elements = GenerateElements(elementNum, elementSize, data);
    std::vector<std::thread> threads;
    for (auto &iter : output) {
        std::string streamName = iter.first;
        int producerNum = iter.second.producers.size();
        iter.second.totalSend.store(0);
        iter.second.totalRecv.store(0);
        for (int i = 0; i < producerNum; i++) {
            threads.emplace_back([&elements, &output, i, streamName, flushIntervals]() {
                auto producer = output[streamName].producers[i].get();
                for (size_t j = 0; j < elements.size(); j++) {
                    ASSERT_EQ(producer->Send(elements[j]), Status::OK());
                    output[streamName].totalSend.fetch_add(1);
                }
                ASSERT_EQ(producer->Close(), Status::OK());
            });
        }

        int totalElementNum = elements.size() * producerNum;
        for (auto &subInfo : iter.second.consumers) {
            auto &subName = subInfo.first;
            int consumerNum = subInfo.second.size();
            for (int i = 0; i < consumerNum; i++) {
                threads.emplace_back([&output, i, streamName, subName, totalElementNum, elementBatchNum, blockingMs]() {
                    int remainNum = totalElementNum;
                    auto consumer = output[streamName].consumers[subName][i].get();
                    while (remainNum > 0) {
                        int expectNum = std::min(elementBatchNum, remainNum);
                        std::vector<Element> outElements;
                        auto status = consumer->Receive(expectNum, blockingMs, outElements);
                        if (status.IsError()) {
                            if (status.GetCode() != StatusCode::K_RUNTIME_ERROR) {
                                LOG(ERROR) << "Receive error:" << status.ToString();
                            }
                        } else if (outElements.empty()) {
                            continue;
                        } else {
                            ASSERT_EQ(consumer->Ack(outElements.back().id), Status::OK());
                        }
                        remainNum -= outElements.size();
                        output[streamName].totalRecv.fetch_add(outElements.size());
                    }
                    ASSERT_EQ(consumer->Close(), Status::OK());
                });
            }
        }
    }
    for (auto &t : threads) {
        t.join();
    }
    for (auto &iter : output) {
        EXPECT_EQ(iter.second.totalSend.load() * iter.second.consumers.size(), iter.second.totalRecv.load());
    }
    return Status::OK();
}

Status MultiProducerMultiConsumerTest::MultiDeleteStream(std::unordered_map<std::string, OutputStreamInfo> &output)
{
    std::vector<std::thread> threads;
    for (auto &iter : output) {
        std::string streamName = iter.first;
        threads.emplace_back([streamName, this]() { ASSERT_EQ(client_->DeleteStream(streamName), Status::OK()); });
    }
    for (auto &t : threads) {
        t.join();
    }
    return Status::OK();
}

std::vector<Element> MultiProducerMultiConsumerTest::GenerateElements(int elementNum, uint64_t elementSize,
                                                                      std::string &outData)
{
    outData = RandomData().GetRandomString(elementNum * elementSize);
    std::vector<Element> ret;
    ret.reserve(elementSize);
    for (int i = 1; i <= elementNum; i++) {
        Element element(reinterpret_cast<uint8_t *>(&outData.front()), elementSize, ULONG_MAX);
        ret.push_back(element);
    }
    return ret;
}

TEST_F(MultiProducerMultiConsumerTest, SPSC)
{
    auto paras = GetInputParas(1, 1, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_10 * K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, SPMC)
{
    auto paras = GetInputParas(1, 1, K_10);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MPSC)
{
    auto paras = GetInputParas(1, K_10, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MPMC)
{
    auto paras = GetInputParas(1, K_3, K_3);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_50, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MSSPSC)
{
    auto paras = GetInputParas(K_10, 1, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MSSPMC)
{
    auto paras = GetInputParas(K_10, 1, K_10);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MSMPSC)
{
    auto paras = GetInputParas(K_5, K_5, 1);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

TEST_F(MultiProducerMultiConsumerTest, MSMPMC)
{
    auto paras = GetInputParas(K_5, K_3, K_3);
    std::unordered_map<std::string, OutputStreamInfo> output;
    AsyncCreateProducersAndConsumers(paras->streams, output);
    MultiSendRecv(output, K_100, K_100, K_10, K_5, K_100);
    MultiDeleteStream(output);
}

// This is set of basic testcases for MPSC scenario (2 producer, 1 consumer)
constexpr int ONE_WORKER = 1;
constexpr int TWO_WORKER = 2;
constexpr int THREE_WORKER = 3;
class BasicMultipleProducerSingleConsumerTest : public SCClientCommon {
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

    void SendHelper(std::shared_ptr<Producer> producer, int numElement)
    {
        const int DEFAULT_SLEEP_TIME = 300;
        int retryLimit = 5;
        size_t sizeElement = 512;

        for (int i = 0; i < numElement; i++) {
            std::vector<uint8_t> writeElement = RandomData().RandomBytes(sizeElement);
            Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

            datasystem::Status rc = producer->Send(element);
            if (rc.IsError()) {
                while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                    rc = producer->Send(element);
                }
            }
            DS_ASSERT_OK(rc);
            int log_interval = 100;
            if (i % log_interval == 0) {
                LOG(INFO) << "send out " << i << " elements";
            }
        }
        LOG(INFO) << "send end";
    }

    void ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements)
    {
        size_t remaining = numElements;
        int round = 0;
        const int roundLimit = 10;
        const int PER_RECEIVE_NUM = 100;
        const int DEFAULT_WAIT_TIME = 1000;
        while (remaining > 0 && round < roundLimit) {
            std::vector<Element> outElements;
            DS_ASSERT_OK(consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements));
            LOG(INFO) << "remaining num : " << remaining;
            LOG(INFO) << "receive num : " << outElements.size() << " ;" << round++;
            if (!outElements.empty()) {
                remaining -= outElements.size();
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
                if (remaining == 0) {
                    break;
                }
            }
        }
    }
    void GetProducer(int numOfWorker, std::shared_ptr<Producer> &Producer,
                     std::string streamName)
    {
        (void)numOfWorker;
        client1_->CreateProducer(streamName, Producer, defaultProducerConf_);
    }

    void GetConsumer(int numOfWorker, std::shared_ptr<Consumer> &Consumer,
                     std::string streamName)
    {
        SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
        switch (numOfWorker) {
            case ONE_WORKER:
                DS_ASSERT_OK(client1_->Subscribe(streamName, config1, Consumer));
                break;
            case TWO_WORKER:
                DS_ASSERT_OK(client2_->Subscribe(streamName, config1, Consumer));
                break;
            case THREE_WORKER:
                DS_ASSERT_OK(client3_->Subscribe(streamName, config1, Consumer));
                break;
        }
    }
    
    void GetProducerConsumers(int numOfWorker, std::shared_ptr<Producer> &Producer1,
        std::shared_ptr<Producer> &Producer2, std::shared_ptr<Consumer> &Consumer1,
        std::string streamName)
    {
        SubscriptionConfig config1("sub1", SubscriptionType::STREAM);

        if (numOfWorker == ONE_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(streamName, Producer1, defaultProducerConf_));
            DS_ASSERT_OK(client1_->CreateProducer(streamName, Producer2, defaultProducerConf_));
            DS_ASSERT_OK(client1_->Subscribe(streamName, config1, Consumer1));
        } else if (numOfWorker == TWO_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(streamName, Producer1, defaultProducerConf_));
            DS_ASSERT_OK(client1_->CreateProducer(streamName, Producer2, defaultProducerConf_));
            DS_ASSERT_OK(client2_->Subscribe(streamName, config1, Consumer1));
        } else if (numOfWorker == THREE_WORKER) {
            DS_ASSERT_OK(client1_->CreateProducer(streamName, Producer1, defaultProducerConf_));
            DS_ASSERT_OK(client2_->CreateProducer(streamName, Producer2, defaultProducerConf_));
            DS_ASSERT_OK(client3_->Subscribe(streamName, config1, Consumer1));
        } else {
            LOG(ERROR) << "Incorrect number of numOfWorker";
            ASSERT_TRUE(false);
        }
    }

    void CloseProducerDuringSend(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "CloseProdDuringSend";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 1;
        int totalRecvNum = 2 * numElement;
        int threadNum = 3;
        ThreadPool pool(threadNum);

        datasystem::inject::Set("ProducerImpl.Send.delay", "call(10)");
        pool.Submit([this, Producer1, numElement]() { SendHelper(Producer1, numElement);});
        pool.Submit([this, Producer2, numElement]() { SendHelper(Producer2, numElement);});
        pool.Submit([this, Consumer1, totalRecvNum]() { ReceiveHelper(Consumer1, totalRecvNum);});

        // wait for create shm page to finish
        const int SLEEP_TIME = 20;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
        DS_ASSERT_OK(Producer1->Close());
    }

    void CloseTwoProducerDuringSendReceive(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "CloseTwoProdDuringSendRecv";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 1;
        int totalRecvNum = 2;
        int threadNum = 3;
        ThreadPool pool(threadNum);
        datasystem::inject::Set("ProducerImpl.Send.delay", "call()");
        pool.Submit([this, Producer1, numElement]() { SendHelper(Producer1, numElement);});
        pool.Submit([this, Producer2, numElement]() { SendHelper(Producer2, numElement);});
        pool.Submit([this, Consumer1, totalRecvNum]() { ReceiveHelper(Consumer1, totalRecvNum);});

        const int SLEEP_TIME = 20;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
        DS_ASSERT_OK(Producer1->Close());
        DS_ASSERT_OK(Producer2->Close());
    }

    void CloseConsumerWhileSend(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "CloseConWhileSend";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 1000;
        int threadNum = 2;
        ThreadPool pool(threadNum);
        pool.Submit([this, Producer1, numElement]() { SendHelper(Producer1, numElement);});
        pool.Submit([this, Producer2, numElement]() { SendHelper(Producer2, numElement);});

        const int SLEEP_TIME = 10;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
        DS_ASSERT_OK(Consumer1->Close());
    }

    void CloseAllWhileSendReceiveDone(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "CloseAllWhileSendRecv";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 500;
        int totalRecvNum = numElement * 2;
        SendHelper(Producer1, numElement);
        SendHelper(Producer2, numElement);
        ReceiveHelper(Consumer1, totalRecvNum);

        DS_ASSERT_OK(Producer1->Close());
        DS_ASSERT_OK(Producer2->Close());
        DS_ASSERT_OK(Consumer1->Close());
    }

    void NewProducerContinueSend(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "NewProducerContinueSend";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 1;
        int totalRecvNum = 3 * numElement;
        int threadNum = 3;
        ThreadPool pool(threadNum);
        datasystem::inject::Set("ProducerImpl.Send.delay", "call(10)");
        pool.Submit([this, Producer1, numElement]() { SendHelper(Producer1, numElement);});
        pool.Submit([this, Producer2, numElement]() { SendHelper(Producer2, numElement);});
        pool.Submit([this, Consumer1, totalRecvNum]() { ReceiveHelper(Consumer1, totalRecvNum);});

        const int SLEEP_TIME = 20;
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
        
        // Close Producer 1, then Create Producer 3 to continue sending
        DS_ASSERT_OK(Producer1->Close());
        std::shared_ptr<Producer> Producer3;
        GetProducer(numOfWorker, Producer3, streamName);
        auto fut = pool.Submit([this, Producer3, numElement]() { SendHelper(Producer3, numElement);});
        fut.get();
    }

    void ConsumerContinueReceive(int numOfWorker)
    {
        std::shared_ptr<Producer> Producer1;
        std::shared_ptr<Producer> Producer2;
        std::shared_ptr<Consumer> Consumer1;
        std::string streamName = "ConsumerContinueReceive";
        GetProducerConsumers(numOfWorker, Producer1, Producer2, Consumer1, streamName);

        int numElement = 1;
        int totalRecvNum = 2 * numElement;
        int threadNum = 3;
        ThreadPool pool(threadNum);
        datasystem::inject::Set("ProducerImpl.Send.delay", "call(100)");
        pool.Submit([this, Producer1, numElement]() { SendHelper(Producer1, numElement);});
        pool.Submit([this, Producer2, numElement]() { SendHelper(Producer2, numElement);});
        
        // Close Consumer 1, then Create Consumer 2 to continue receiving
        DS_ASSERT_OK(Consumer1->Close());
        std::shared_ptr<Consumer> Consumer2;
        GetConsumer(numOfWorker, Consumer2, streamName);
        ReceiveHelper(Consumer2, totalRecvNum);
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
        LOG(INFO) << FormatString("\n Worker1: <%s>\n Worker2: <%s>\n Worker3: <%s>", workerAddress1.ToString(),
                                  workerAddress2.ToString(), workerAddress3.ToString());
        InitStreamClient(0, client1_);
        InitStreamClient(1, client2_);
        InitStreamClient(2, client3_);  // worker index is 2
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }
    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";

    // cluster config
    int numWorkers = 3;
    int numEtcd = 1;
    int numRpcThreads = 0;
};

// these testsuites test all scenarios in one testcase, 1 worker, 2 worker and 3 worker
TEST_F(BasicMultipleProducerSingleConsumerTest, CloseProducerDuringSend)
{
    for (int i = 1; i <= numWorkers; i++) {
        CloseProducerDuringSend(i);
    }
}

TEST_F(BasicMultipleProducerSingleConsumerTest, CloseTwoProducerDuringSendReceive)
{
    for (int i = 1; i <= numWorkers; i++) {
        CloseTwoProducerDuringSendReceive(i);
    }
}

TEST_F(BasicMultipleProducerSingleConsumerTest, CloseConsumerWhileSend)
{
    for (int i = 1; i <= numWorkers; i++) {
        CloseConsumerWhileSend(i);
    }
}

TEST_F(BasicMultipleProducerSingleConsumerTest, CloseAllWhileSendReceiveDone)
{
    for (int i = 1; i <= numWorkers; i++) {
        CloseAllWhileSendReceiveDone(i);
    }
}

TEST_F(BasicMultipleProducerSingleConsumerTest, NewProducerContinueSend)
{
    for (int i = 1; i <= numWorkers; i++) {
        NewProducerContinueSend(i);
    }
}

TEST_F(BasicMultipleProducerSingleConsumerTest, ConsumerContinueReceive)
{
    for (int i = 1; i <= numWorkers; i++) {
        ConsumerContinueReceive(i);
    }
}

}  // namespace st
}  // namespace datasystem

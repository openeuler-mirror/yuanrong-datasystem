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
 * Description: Remote send test.
 */
#include <mutex>

#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
class RemoteMemCtrlTest : public SCClientCommon {
public:
    RemoteMemCtrlTest(uint32_t maxStreamSizeMb = 2, int64_t pageSize = 1024 * 4)
        : maxStreamSizeMb_(maxStreamSizeMb), pageSize_(pageSize), bigSize_(pageSize_ / 16)
    {
        const uint64_t MB = 1024 * 1024;
        producerConf_.pageSize = pageSize;
        producerConf_.maxStreamSize = maxStreamSizeMb * MB;
    }
    virtual void SetClusterSetupOptions(ExternalClusterOptions &opts) override;

    void SetUp() override;

    void TearDown() override;

    static std::string streamName_;
    static std::once_flag onceFlag_;

protected:
    static Status Produce(std::shared_ptr<Producer> &producer, std::string producerName, size_t flushIntervals,
                          size_t numElements, uint64_t maxEleSz, uint64_t minEleSz, uint64_t *res = nullptr);

    static Status ConsumeAll(std::shared_ptr<Consumer> &consumer, size_t numElements, int batchNum, int ackInterval,
                             int sleepAfterRecvUs, int timeout = 5000, uint64_t *res = nullptr);

    static Status ConsumeAllClose(std::shared_ptr<Consumer> &consumer, size_t numElements, int batchNum,
                                  int ackInterval, int sleepAfterRecv, int timeout = 5000, uint64_t *res = nullptr);

    void BasicSPSC(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements, int recvBatchNum,
                   int recvTimeout, int ackInterval, int sleepAfterRecvUs);

    void SendSideMultiProducers(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                                int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs);

    void RecvSideAddConsumer(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                             int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs);

    void SendSideConsumer(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                          int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs);

    void BothDirection(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                       int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs);

    uint32_t maxStreamSizeMb_;
    uint64_t pageSize_;
    uint64_t bigSize_;

    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;
    HostPort w3Addr_;

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    std::shared_ptr<StreamClient> w3Client_ = nullptr;
    ProducerConf producerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};
std::string RemoteMemCtrlTest::streamName_ = "stream";
std::once_flag RemoteMemCtrlTest::onceFlag_;

void RemoteMemCtrlTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = 3;
    opts.workerGflagParams = " -page_size=" + std::to_string(pageSize_);
    opts.vLogLevel = 2;
    SCClientCommon::SetClusterSetupOptions(opts);
}

void RemoteMemCtrlTest::SetUp()
{
    ExternalClusterTest::SetUp();
    InitStreamClient(0, w1Client_);
    InitStreamClient(0, w2Client_);
    InitStreamClient(0, w3Client_);
}

void RemoteMemCtrlTest::TearDown()
{
    w1Client_ = nullptr;
    w2Client_ = nullptr;
    w3Client_ = nullptr;
    ExternalClusterTest::TearDown();
}

Status RemoteMemCtrlTest::Produce(std::shared_ptr<Producer> &producer, std::string producerName, size_t flushIntervals,
                                  size_t numElements, uint64_t maxEleSz, uint64_t minEleSz, uint64_t *res)
{
    (void)flushIntervals;
    uint64_t totalEleSz = 0;
    ElementGenerator elementGenerator(maxEleSz, minEleSz);
    auto strs = elementGenerator.GenElements(producerName, numElements);
    for (size_t i = 0; i < numElements; i++) {
        totalEleSz += strs[i].size();
        Status status;
        int retry = 0;
        int cnt = 0;
        do {
            ++retry;
            status = producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size()));
            if (status.IsError()) {
                if (cnt % 30 == 0) {
                    LOG(ERROR) << "Send error:" << status.ToString() << ", with retry: " << retry;
                }
                cnt++;
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        } while (status.IsError() && status.GetCode() == StatusCode::K_OUT_OF_MEMORY);
        RETURN_IF_NOT_OK(status);
    }
    if (res) {
        *res = totalEleSz;
    }
    RETURN_IF_NOT_OK(producer->Close());
    return Status::OK();
}

Status RemoteMemCtrlTest::ConsumeAll(std::shared_ptr<Consumer> &consumer, size_t numElements, int batchNum,
                                     int ackInterval, int sleepAfterRecvUs, int timeout, uint64_t *res)
{
    std::vector<Element> outElements;
    int remainNum = numElements;
    std::unordered_map<std::string, uint64_t> seqNoMap;
    uint64_t eleTotalSz = 0;
    std::unordered_map<std::string, std::vector<uint64_t>> seqNums;
    while (remainNum > 0) {
        int expectNum = std::min(batchNum, remainNum);
        std::vector<Element> out;
        RETURN_IF_NOT_OK(consumer->Receive(expectNum, timeout, out));
        for (const auto &element : out) {
            std::string info = FormatString("Element ID %zu :", element.id);
            ElementView view(std::string((const char *)element.ptr, element.size));
            RETURN_IF_NOT_OK_APPEND_MSG(view.VerifyIntegrity(), info);
            RETURN_IF_NOT_OK_APPEND_MSG(view.VerifyFifo(seqNoMap, 0), info);
            eleTotalSz += element.size;
            uint64_t seqNo;
            RETURN_IF_NOT_OK(view.GetSeqNo(seqNo));
            std::string producerId;
            RETURN_IF_NOT_OK(view.GetProducerId(producerId));
            seqNums[std::string(producerId)].push_back(seqNo);
        }
        outElements.insert(outElements.end(), out.begin(), out.end());
        remainNum -= out.size();
        if (sleepAfterRecvUs > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sleepAfterRecvUs));
        }
        if (outElements.size() % ackInterval == 0) {
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
        }
    }
    for (auto &entry : seqNums) {
        LOG(INFO) << FormatString("Receive form producer[%s], nums:[%zu].", entry.first, entry.second.size());
    }
    LOG(INFO) << "Actual got element size:" << outElements.size();
    CHECK_FAIL_RETURN_STATUS(
        outElements.size() == numElements, StatusCode::K_RUNTIME_ERROR,
        FormatString("Should recv %zu elements, actual recv %zu elements", numElements, outElements.size()));
    if (res != nullptr) {
        *res = eleTotalSz;
    }
    return Status::OK();
}

Status RemoteMemCtrlTest::ConsumeAllClose(std::shared_ptr<Consumer> &consumer, size_t numElements, int batchNum,
                                          int ackInterval, int sleepAfterRecv, int timeout, uint64_t *res)
{
    RETURN_IF_NOT_OK(ConsumeAll(consumer, numElements, batchNum, ackInterval, sleepAfterRecv, timeout, res));
    return consumer->Close();
}

void RemoteMemCtrlTest::BasicSPSC(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                                  int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 2; i++) {
        uint64_t sendDataSz = 0;
        uint64_t recvDataSz = 0;
        std::vector<std::future<Status>> futs;
        std::promise<void> promise;
        std::vector<std::shared_ptr<Producer>> producers(1);
        // W1: 1 producer
        futs.emplace_back(pool.Submit([this, &promise, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz,
                                       &sendDataSz, &producers, i]() {
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, producerConf_));
            LOG(INFO) << "Succeed CreateProducer with stream " << streamName;
            promise.get_future().get();
            // Send.
            auto producerId = "producer" + std::to_string(i);
            RETURN_IF_NOT_OK(
                Produce(producer, producerId, flushIntervals, numElements, maxEleSz, minEleSz, &sendDataSz));
            producers[0] = std::move(producer);
            return Status::OK();
        }));
        // W2: 1 consumer
        futs.emplace_back(pool.Submit([this, &promise, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout, &recvDataSz]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            promise.set_value();
            RETURN_IF_NOT_OK(ConsumeAllClose(consumer, numElements, recvBatchNum, ackInterval, sleepAfterRecvUs,
                                             recvTimeout, &recvDataSz));
            return Status::OK();
        }));
        for (auto &fut : futs) {
            ASSERT_EQ(fut.get(), Status::OK());
        }
        EXPECT_EQ(sendDataSz, recvDataSz);
        LOG(INFO) << FormatString("Finish: %d, sendDataSz:%zu, recvDataSz:%zu", i, sendDataSz, recvDataSz);
        ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
    }
}

// Recv slower than send and all elements is small element.
// W1: Producer.
// W2: Consumer.
TEST_F(RemoteMemCtrlTest, TestBasicSPSC0)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { BasicSPSC(round, 1, 1, 10, 1000, 1, 30'000, 10, 0); });
    }
}

// Recv slower than send and partial elements are big elements.
// W1: Producer.
// W2: Consumer.
TEST_F(RemoteMemCtrlTest, TestBasicSPSC1)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { BasicSPSC(round, 1 * KB, 2 * KB, 10, 1000, 1, 30'000, 10, 5'000); });
    }
}

void RemoteMemCtrlTest::SendSideMultiProducers(int round, int minEleSz, int maxEleSz, size_t flushIntervals,
                                               size_t numElements, int recvBatchNum, int recvTimeout, int ackInterval,
                                               int sleepAfterRecvUs)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 1; i++) {
        std::atomic<uint64_t> sendDataSz = { 0 };
        uint64_t recvDataSz = 0;
        std::vector<std::future<Status>> futs;
        std::promise<void> promise;
        std::shared_future<void> sFut = promise.get_future();
        std::vector<std::shared_ptr<Producer>> producers(2);
        for (auto j = 0; j < 2; j++) {
            futs.emplace_back(pool.Submit([this, &sFut, j, streamName, &flushIntervals, &numElements, &maxEleSz,
                                           &minEleSz, &sendDataSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, producerConf_));
                sFut.get();
                std::string producerName = FormatString("producer-[%d]", j);
                LOG(INFO) << FormatString("The producer name:%s, ", producerName);
                // Send.
                uint64_t sendData = 0;
                auto producerId = "producer" + std::to_string(j);
                RETURN_IF_NOT_OK(
                    Produce(producer, producerId, flushIntervals, numElements, maxEleSz, minEleSz, &sendData));
                sendDataSz.fetch_add(sendData);
                producers[j] = std::move(producer);
                return Status::OK();
            }));
        }
        // W2: 1 consumer
        futs.emplace_back(pool.Submit([this, &promise, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout, &recvDataSz]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            promise.set_value();
            RETURN_IF_NOT_OK(ConsumeAllClose(consumer, numElements * 2, recvBatchNum, ackInterval, sleepAfterRecvUs,
                                             recvTimeout, &recvDataSz));
            return Status::OK();
        }));
        for (auto &fut : futs) {
            ASSERT_EQ(fut.get(), Status::OK());
        }
        ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
    }
}

// W1: Two producers.
// W2: Consumer.
// Flush need FIFO for a producer.
TEST_F(RemoteMemCtrlTest, LEVEL1_TestSendSideMultiProducers)
{
    auto rounds = 10;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { SendSideMultiProducers(round, 1, 1, 10, 1000, 1, 30'000, 10, 0); });
    }
}

void RemoteMemCtrlTest::RecvSideAddConsumer(int round, int minEleSz, int maxEleSz, size_t flushIntervals,
                                            size_t numElements, int recvBatchNum, int recvTimeout, int ackInterval,
                                            int sleepAfterRecvUs)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 10; i++) {
        std::vector<std::future<Status>> futs;
        std::future<Status> futs1;
        std::promise<void> promise;
        // Get shared future form promise, it can be get multi times.
        std::shared_future<void> sfut = promise.get_future();
        std::vector<std::shared_ptr<Producer>> producers(1);

        // W1: P1, send data after C1 created.
        futs.emplace_back(
            pool.Submit([this, &sfut, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, producerConf_));
                sfut.get();

                // Send.
                RETURN_IF_NOT_OK(Produce(producer, "producer", flushIntervals, numElements, maxEleSz, minEleSz));
                producers[0] = std::move(producer);
                return Status::OK();
            }));

        // W2: C1
        futs1 = pool.Submit(
            [this, &promise, streamName, &numElements, &recvBatchNum, &ackInterval, &sleepAfterRecvUs, &recvTimeout]() {
                std::shared_ptr<Consumer> consumer;
                SubscriptionConfig config("sub1", SubscriptionType::STREAM);
                RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
                promise.set_value();
                RETURN_IF_NOT_OK(
                    ConsumeAllClose(consumer, numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout));
                return Status::OK();
            });

        // W2: C2, create C2 after C1 created.
        futs.emplace_back(pool.Submit([this, &futs1, &sfut, streamName, &numElements]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub2", SubscriptionType::STREAM);
            sfut.get();
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));

            std::vector<Element> outElements;
            std::unordered_map<std::string, uint64_t> seqNoMap;
            size_t recvNum = 0;
            while (!IsThreadFinished(futs1, 0)) {
                auto stat = consumer->Receive(1, 0, outElements);
                if (stat == Status::OK() && !outElements.empty()) {
                    const auto &e = outElements.back();
                    LOG(INFO) << "Cursor: " << e.id << ", Sz: " << e.size;
                    ElementView view(std::string((const char *)e.ptr, e.size));
                    RETURN_IF_NOT_OK(view.VerifyIntegrity());
                    RETURN_IF_NOT_OK(view.VerifyFifoInitOff(seqNoMap));
                    recvNum++;
                }
            }
            CHECK_FAIL_RETURN_STATUS(recvNum <= numElements, StatusCode::K_RUNTIME_ERROR, "");
            return consumer->Close();
        }));
        for (auto &fut : futs) {
            ASSERT_EQ(fut.get(), Status::OK());
        }
        futs1.get();
        ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
        LOG(INFO) << FormatString("Finish: %d", i);
    }
}

// W1: Producer.
// W: Consumer, then dynamically add another.
TEST_F(RemoteMemCtrlTest, TestRecvSideAddConsumer)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { RecvSideAddConsumer(round, 1, 1, 10, 1000, 1, 30'000, 10, 0); });
    }
}

void RemoteMemCtrlTest::SendSideConsumer(int round, int minEleSz, int maxEleSz, size_t flushIntervals,
                                         size_t numElements, int recvBatchNum, int recvTimeout, int ackInterval,
                                         int sleepAfterRecvUs)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 10; i++) {
        std::vector<std::future<Status>> futs;
        std::vector<std::promise<void>> promises(3);
        std::vector<std::shared_future<void>> sFuts;  // Can be get multi times
        for (auto &promise : promises) {
            sFuts.emplace_back(promise.get_future());
        }
        std::vector<std::shared_ptr<Producer>> producers(1);
        // W1: C1, can receive all elements.
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config, consumer));
            promises[0].set_value();

            RETURN_IF_NOT_OK(
                ConsumeAllClose(consumer, numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout));
            return Status::OK();
        }));
        // W1: C2, can receive all elements.
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub2", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config, consumer));
            promises[1].set_value();
            return ConsumeAllClose(consumer, numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout);
        }));
        // W1: P1, Send data after c1,c2,c3 create successfully.
        futs.emplace_back(
            pool.Submit([this, &sFuts, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, producerConf_));

                // Send, wait c1,c2,c3 create successfully.
                for (auto &sFut : sFuts) {
                    sFut.get();
                }
                RETURN_IF_NOT_OK(Produce(producer, "producer", flushIntervals, numElements, maxEleSz, minEleSz));
                producers[0] = std::move(producer);
                return Status::OK();
            }));
        // W2: C3, can receive all elements.
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub3", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;

            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            promises[2].set_value();
            RETURN_IF_NOT_OK(
                ConsumeAllClose(consumer, numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout));
            return Status::OK();
        }));
        for (auto &fut : futs) {
            ASSERT_EQ(fut.get(), Status::OK());
        }
        ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
        LOG(INFO) << FormatString("Finish: %d", i);
    }
}

// W1: Producer, C1, C2.
// W2: Consumer.
TEST_F(RemoteMemCtrlTest, TestSendSideConsumer)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { SendSideConsumer(round, 1, 1, 10, 1000, 1, 30'000, 10, 0); });
    }
}

void RemoteMemCtrlTest::BothDirection(int round, int minEleSz, int maxEleSz, size_t flushIntervals, size_t numElements,
                                      int recvBatchNum, int recvTimeout, int ackInterval, int sleepAfterRecvUs)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 10; i++) {
        LOG(INFO) << FormatString("===================== [Round: %d] Start =====================", i);
        std::vector<std::future<Status>> futs;
        std::vector<std::promise<void>> promises(3);
        std::vector<std::shared_future<void>> sFuts;
        for (auto &promise : promises) {
            sFuts.emplace_back(promise.get_future());
        }
        std::vector<std::shared_ptr<Producer>> producers(3);
        // W1 : P1
        futs.emplace_back(
            pool.Submit([this, &sFuts, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, producerConf_));
                // Wait for all consumers created successfully.
                for (auto &sFut : sFuts) {
                    sFut.get();
                }
                RETURN_IF_NOT_OK(Produce(producer, "producer1", flushIntervals, numElements, maxEleSz, minEleSz));
                producers[0] = std::move(producer);
                return Status::OK();
            }));
        // W2: P2
        futs.emplace_back(
            pool.Submit([this, &sFuts, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w2Client_->CreateProducer(streamName, producer, producerConf_));
                // Wait for all consumers created successfully.
                for (auto &sFut : sFuts) {
                    sFut.get();
                }
                RETURN_IF_NOT_OK(Produce(producer, "producer2", flushIntervals, numElements, maxEleSz, minEleSz));
                producers[1] = std::move(producer);
                return Status::OK();
            }));
        // W3: P3
        futs.emplace_back(
            pool.Submit([this, &sFuts, streamName, &flushIntervals, &numElements, &maxEleSz, &minEleSz, &producers]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w3Client_->CreateProducer(streamName, producer, producerConf_));
                // Wait for all consumers created successfully.
                for (auto &sFut : sFuts) {
                    sFut.get();
                }
                RETURN_IF_NOT_OK(Produce(producer, "producer3", flushIntervals, numElements, maxEleSz, minEleSz));
                producers[2] = std::move(producer);
                return Status::OK();
            }));
        // W1: C1, C2
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config, consumer));
            promises[0].set_value();  // Notify producers to send elements

            return ConsumeAllClose(consumer, 3 * numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout);
        }));
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub2", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config, consumer));
            promises[1].set_value();  // Notify producers to send elements

            return ConsumeAllClose(consumer, 3 * numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout);
        }));

        // W2: C3
        futs.emplace_back(pool.Submit([this, &promises, streamName, &numElements, &recvBatchNum, &ackInterval,
                                       &sleepAfterRecvUs, &recvTimeout]() {
            SubscriptionConfig config("sub3", SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            promises[2].set_value();  // Notify producers to send elements
            return ConsumeAllClose(consumer, 3 * numElements, recvBatchNum, ackInterval, sleepAfterRecvUs, recvTimeout);
        }));
        for (auto &fut : futs) {
            ASSERT_EQ(fut.get(), Status::OK());
        }
        ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
        LOG(INFO) << FormatString("Finish: %d", i);
        LOG(INFO) << FormatString("===================== [Round: %d] End =====================", i);
    }
}

// W1: Producer, 2Consumer.
// W2: Producer, Consumer.
// W3: Producer.
TEST_F(RemoteMemCtrlTest, TestBothDirection)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Submit([this, round]() { BothDirection(round, 1, 1, 10, 1000, 1, 30'000, 10, 0); });
    }
}

}  // namespace st
}  // namespace datasystem

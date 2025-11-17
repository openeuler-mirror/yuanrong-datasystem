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
 * Description: Remote send test with shared page queue enabled.
 */
#include <mutex>
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/random_data.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
constexpr int K_TWO = 2;
constexpr int K_TEN = 10;
constexpr int K_TWENTY = 20;
class SharedPageSendRecvTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;

    void SetUp() override;

    void TearDown() override;

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

    Status SendHelper(std::shared_ptr<Producer> producer, size_t numElements, Element element);
    Status SendRandomHelper(std::shared_ptr<Producer> producer, size_t numElements, const std::string &data,
                            size_t minSize = 1024);
    Status ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements, const std::string &expectedData = "");

    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;
    HostPort w3Addr_;

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    std::shared_ptr<StreamClient> w3Client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const int DEFAULT_WAIT_TIME = 5000;
    const int DEFAULT_WORKER_NUM = 3;
    const int DEFAULT_LOG_LEVEL = 2;
};

void SharedPageSendRecvTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = DEFAULT_WORKER_NUM;
    opts.enableDistributedMaster = "true";
    opts.numRpcThreads = 0;
    opts.vLogLevel = DEFAULT_LOG_LEVEL;
    opts.workerGflagParams = " -shared_memory_size_mb=2048 ";

    SCClientCommon::SetClusterSetupOptions(opts);
}

void SharedPageSendRecvTest::SetUp()
{
    ExternalClusterTest::SetUp();
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(K_TWO, w3Addr_));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, K_TWO));
    // Worker 1.
    InitStreamClient(0, w1Client_);
    InitStreamClient(1, w2Client_);
    InitStreamClient(K_TWO, w3Client_);
    defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
}

void SharedPageSendRecvTest::TearDown()
{
    w1Client_ = nullptr;
    w2Client_ = nullptr;
    w3Client_ = nullptr;
    ExternalClusterTest::TearDown();
}

Status SharedPageSendRecvTest::SendHelper(std::shared_ptr<Producer> producer, size_t numElements, Element element)
{
    const int DEFAULT_SLEEP_TIME = 300;
    int retryLimit = 30;
    for (size_t i = 0; i < numElements; i++) {
        datasystem::Status rc = producer->Send(element);
        if (rc.IsError()) {
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
        }
        RETURN_IF_NOT_OK(rc);
    }
    return Status::OK();
}

Status SharedPageSendRecvTest::SendRandomHelper(std::shared_ptr<Producer> producer, size_t numElements,
                                                const std::string &data, size_t minSize)
{
    const int DEFAULT_SLEEP_TIME = 300;
    const int DEFAULT_RETRY_TIME = 60;
    Timer timer;
    size_t maxSize = data.size();
    minSize = std::min(maxSize, minSize);
    for (size_t i = 0; i < numElements; i++) {
        size_t sizeElement = RandomData().GetRandomUint32(minSize, maxSize);
        std::string writeElement = data.substr(0, sizeElement);
        Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

        datasystem::Status rc = producer->Send(element);
        if (rc.IsError()) {
            while (rc.GetCode() == K_OUT_OF_MEMORY && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
        }
        if (rc.IsError()) {
            LOG(INFO) << "send failed exits.";
        }
        RETURN_IF_NOT_OK(rc);
        LOG(INFO) << "send count:" << i;
    }
    return Status::OK();
}

Status SharedPageSendRecvTest::ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements,
                                             const std::string &expectedData)
{
    Timer timer;
    size_t remaining = numElements;
    int round = 0;
    const int PER_RECEIVE_NUM = 1;
    const int DEFAULT_WAIT_TIME = 1000;
    const int DEFAULT_RETRY_TIME = 30;
    while (remaining > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
        std::vector<Element> outElements;
        RETURN_IF_NOT_OK(consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements));
        LOG(INFO) << "remaining num : " << remaining << ", receive num : " << outElements.size() << " ;" << round++;
        if (!outElements.empty()) {
            remaining -= outElements.size();
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            if (!expectedData.empty()) {
                std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
                CHECK_FAIL_RETURN_STATUS(expectedData == actualData, K_RUNTIME_ERROR,
                                         "expected data does not match actual data.");
            }
        }
    }
    CHECK_FAIL_RETURN_STATUS(remaining == 0, K_RUNTIME_ERROR, "failed to receive all data");
    return Status::OK();
}

TEST_F(SharedPageSendRecvTest, TestBasicSingleStream)
{
    // Test that a single stream with shared page enabled, send and receive are working as expected.
    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());

    const int threadNum = 2;
    // shared page is default of size 4MB, so apply ~10MB data to test that multiple pages would work
    const size_t numElements = 10000;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
}

TEST_F(SharedPageSendRecvTest, TestBasicMultiStream)
{
    // Test that with 3 streams with consumer on the same node, the shared page logic is working fine.

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream3", producer3, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 10000;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    futs.push_back(
        pool.Submit([this, producer3, &element3]() { return SendHelper(producer3, numElements, element3); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));
    futs.push_back(pool.Submit(
        [this, consumer3, &writeElement3]() { return ReceiveHelper(consumer3, numElements, writeElement3); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream3"));
}

TEST_F(SharedPageSendRecvTest, TestCloseOneStreamConsumer)
{
    // Test that if one of the consumer is closed in shared page case, the buffers are discarded correctly.

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream3", producer3, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 10000;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    futs.push_back(
        pool.Submit([this, producer3, &element3]() { return SendHelper(producer3, numElements, element3); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));

    sleep(1);
    DS_ASSERT_OK(consumer3->Close());

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream3"));
}

TEST_F(SharedPageSendRecvTest, TestDeleteOneStream)
{
    // Test that if one of the streams is deleted in shared page case, the buffers are discarded correctly.

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream3", producer3, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 10000;
    // It will be slow if the stream to be deleted sends too many elements,
    // so only send 1000 elements to get discarded.
    const size_t numElementsForDeleteStream = 1000;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    futs.push_back(pool.Submit([this, producer3, consumer3, &element3]() {
        RETURN_IF_NOT_OK(SendHelper(producer3, numElementsForDeleteStream, element3));
        // Delete stream after the send is done, so the data should be discarded.
        RETURN_IF_NOT_OK(producer3->Close());
        RETURN_IF_NOT_OK(consumer3->Close());
        RETURN_IF_NOT_OK(TryAndDeleteStream(w1Client_, "stream3"));
        return Status::OK();
    }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
}

TEST_F(SharedPageSendRecvTest, TestMultiStreamStreamNoTiming)
{
    // Test that with 3 streams with parallel create producer, stream numbers are generated correctly.
    // Injection is used for the timing.
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 0, "ClientWorkerSCServiceImpl.CreateStreamManagerImpl.StreamNo_Sleep", "sleep(2000)"));
    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Producer> producer3;
    const int NUM_PRODUCERS = 3;
    ThreadPool createProducerPool(NUM_PRODUCERS);
    std::vector<std::future<Status>> futs;
    // CreateProducer in parallel to trigger the potential timing.
    futs.emplace_back(createProducerPool.Submit(
        [this, &producer1, conf]() { return w1Client_->CreateProducer("stream1", producer1, conf); }));
    futs.emplace_back(createProducerPool.Submit(
        [this, &producer2, conf]() { return w1Client_->CreateProducer("stream2", producer2, conf); }));
    futs.emplace_back(createProducerPool.Submit(
        [this, &producer3, conf]() { return w1Client_->CreateProducer("stream3", producer3, conf); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    futs.clear();

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 10000;
    ThreadPool sendRecvPool(threadNum);
    futs.push_back(
        sendRecvPool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        sendRecvPool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    futs.push_back(
        sendRecvPool.Submit([this, producer3, &element3]() { return SendHelper(producer3, numElements, element3); }));
    futs.push_back(sendRecvPool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(sendRecvPool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));
    futs.push_back(sendRecvPool.Submit(
        [this, consumer3, &writeElement3]() { return ReceiveHelper(consumer3, numElements, writeElement3); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream3"));
}

TEST_F(SharedPageSendRecvTest, TestShutdownWithoutDelStream)
{
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
}

TEST_F(SharedPageSendRecvTest, TestReuseSharedPageQueue)
{
    // Test that shared page queue can be reused, when stream is deleted and re-created.
    auto func = [this]() {
        // Start consumers first for now.
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        std::shared_ptr<Consumer> consumer1;
        RETURN_IF_NOT_OK(w2Client_->Subscribe("stream1", config, consumer1));

        ProducerConf conf;
        conf.maxStreamSize = TEST_STREAM_SIZE;
        conf.streamMode = StreamMode::SPSC;
        std::shared_ptr<Producer> producer1;
        RETURN_IF_NOT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
        const size_t sizeElement = 1 * KB;
        std::string writeElement1 = RandomData().GetRandomString(sizeElement);
        Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());

        const int threadNum = 2;
        // shared page is default of size 4MB, so apply ~10MB data to test that multiple pages would work
        const size_t numElements = 10000;
        ThreadPool pool(threadNum);
        std::vector<std::future<Status>> futs;
        futs.push_back(
            pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
        futs.push_back(pool.Submit(
            [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));

        for (auto &fut : futs) {
            RETURN_IF_NOT_OK(fut.get());
        }
        RETURN_IF_NOT_OK(producer1->Close());
        RETURN_IF_NOT_OK(consumer1->Close());
        RETURN_IF_NOT_OK(TryAndDeleteStream(w1Client_, "stream1"));
        return Status::OK();
    };
    DS_ASSERT_OK(func());
    // Recreate stream so it uses the same shared page queue.
    DS_ASSERT_OK(func());
}

TEST_F(SharedPageSendRecvTest, TestOneStreamBlocking1)
{
    // Test that if one of the streams got OOM and blocked,
    // the data will be moved to separate shm blocks,
    // and the shared pages can be acked and freed.
    // In this testcase, trigger blocking first and then send data of other streams.

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    // Restrict the stream size, so that blocking happens earlier for stream3.
    const uint64_t maxStreamSize = 8 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream3", producer3, conf));

    const size_t sizeElement = 250 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 100;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    // Send stream3 data first to trigger blocking before hand.
    auto s3pFut = pool.Submit([this, producer3, &element3]() { return SendHelper(producer3, numElements, element3); });
    // Wait for push and move to happen and then continue to other streams.
    sleep(1);
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    // Test that the blocked data can still all be received once unblocked.
    DS_ASSERT_OK(ReceiveHelper(consumer3, numElements, writeElement3));
    DS_ASSERT_OK(s3pFut.get());

    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream3"));
}

TEST_F(SharedPageSendRecvTest, TestOneStreamBlocking2)
{
    // Test that if one of the streams got OOM and blocked,
    // the data will be moved to separate shm blocks,
    // and the shared pages can be acked and freed.
    // In this testcase, send elements from streams together.

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w2Client_->Subscribe("stream2", config, consumer2));
    std::shared_ptr<Consumer> consumer3;
    DS_ASSERT_OK(w2Client_->Subscribe("stream3", config, consumer3));

    ProducerConf conf;
    // Restrict the stream size, so that blocking happens earlier for stream3.
    const uint64_t maxStreamSize = 8 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer2, conf));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream3", producer3, conf));

    const size_t sizeElement = 250 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());
    std::string writeElement3 = RandomData().GetRandomString(sizeElement);
    Element element3(reinterpret_cast<uint8_t *>(writeElement3.data()), writeElement3.size());

    const int threadNum = 6;
    const size_t numElements = 100;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    // Send stream3 data first to trigger blocking before hand.
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element2]() { return SendHelper(producer2, numElements, element2); }));
    auto s3pFut = pool.Submit([this, producer3, &element3]() { return SendHelper(producer3, numElements, element3); });
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, numElements, writeElement2); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    // Test that the blocked data can still all be received once unblocked.
    DS_ASSERT_OK(ReceiveHelper(consumer3, numElements, writeElement3));
    DS_ASSERT_OK(s3pFut.get());
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(consumer3->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream3"));
}

TEST_F(SharedPageSendRecvTest, TestProduderAndConsuemrAtOneNode)
{
    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.streamMode = StreamMode::MPSC;
    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));
    DS_ASSERT_OK(w2Client_->CreateProducer("stream1", producer2, conf));

    // Start consumers first for now.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w1Client_->Subscribe("stream1", config, consumer1));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(w1Client_->Subscribe("stream2", config, consumer2));

    std::shared_ptr<Producer> producer3;
    std::shared_ptr<Producer> producer4;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream2", producer3, conf));
    DS_ASSERT_OK(w2Client_->CreateProducer("stream2", producer4, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());
    std::string writeElement2 = RandomData().GetRandomString(sizeElement);
    Element element2(reinterpret_cast<uint8_t *>(writeElement2.data()), writeElement2.size());

    const int threadNum = 6;
    const size_t numElements = 10000;
    const size_t recvNumElements = numElements * 2;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer2, &element1]() { return SendHelper(producer2, numElements, element1); }));
    futs.push_back(
        pool.Submit([this, producer3, &element2]() { return SendHelper(producer3, numElements, element2); }));
    futs.push_back(
        pool.Submit([this, producer4, &element2]() { return SendHelper(producer4, numElements, element2); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, recvNumElements, writeElement1); }));
    futs.push_back(pool.Submit(
        [this, consumer2, &writeElement2]() { return ReceiveHelper(consumer2, recvNumElements, writeElement2); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(producer3->Close());
    DS_ASSERT_OK(producer4->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(consumer2->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream2"));
}

TEST_F(SharedPageSendRecvTest, TestCloseLastProducerWhenDataSending)
{
    ProducerConf prodConf;
    prodConf.maxStreamSize = TEST_STREAM_SIZE;
    prodConf.streamMode = StreamMode::MPSC;
    SubscriptionConfig subsConfig("sub1", SubscriptionType::STREAM);
    std::vector<StreamClient *> clients = { w1Client_.get(), w2Client_.get(), w3Client_.get() };

    const size_t sizeElement = 50 * KB;
    const size_t numElements = 300;
    std::string writeElement = RandomData().GetRandomString(sizeElement);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    auto start = [this, &clients, &prodConf, &subsConfig, &writeElement, &element](const std::string &streamName,
                                                                                   size_t consumerIndex) {
        std::vector<std::shared_ptr<Producer>> producers;
        std::shared_ptr<Consumer> consumer;
        for (size_t index = 0; index < clients.size(); index++) {
            auto client = clients[index];
            if (consumerIndex == index) {
                RETURN_IF_NOT_OK(client->Subscribe(streamName, subsConfig, consumer));
            } else {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, prodConf));
                producers.emplace_back(std::move(producer));
            }
        }
        ThreadPool pool(clients.size());
        std::vector<std::future<Status>> futs;
        for (auto &producer : producers) {
            futs.push_back(pool.Submit([this, producer, &element]() {
                RETURN_IF_NOT_OK(SendHelper(producer, numElements, element));
                RETURN_IF_NOT_OK(producer->Close());
                return Status::OK();
            }));
        }
        size_t recvNumElements = numElements * producers.size();
        futs.push_back(pool.Submit([this, consumer, &writeElement, recvNumElements]() {
            return ReceiveHelper(consumer, recvNumElements, writeElement);
        }));
        Status lastRc;
        for (auto &fut : futs) {
            auto rc = fut.get();
            if (rc.IsError()) {
                lastRc = rc;
            }
        }
        return lastRc;
    };

    const int testStreamCount = 3;
    for (int i = 0; i < testStreamCount; i++) {
        std::string streamName = "stream-" + std::to_string(i);
        DS_ASSERT_OK(start(streamName, i % clients.size()));
    }
}

TEST_F(SharedPageSendRecvTest, TestRemoteRecvIncUsageFailed)
{
    const int maxStreamSize = 4 * MB;
    const int pageSize = 1 * MB;
    ProducerConf prodConf;
    prodConf.maxStreamSize = maxStreamSize;
    prodConf.pageSize = pageSize;
    prodConf.retainForNumConsumers = 1;
    prodConf.streamMode = StreamMode::MPSC;
    SubscriptionConfig subsConfig("sub1", SubscriptionType::STREAM);
    std::vector<StreamClient *> clients = { w1Client_.get(), w2Client_.get(), w3Client_.get() };

    const size_t numElements = 300;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamMetaShm.TryIncUsage", "5%return(K_OUT_OF_MEMORY)"));
    auto start = [this, &clients, &prodConf, &subsConfig](const std::string &streamName, size_t consumerIndex) {
        std::vector<std::shared_ptr<Producer>> producers;
        std::shared_ptr<Consumer> consumer;
        for (size_t index = 0; index < clients.size(); index++) {
            auto client = clients[index];
            if (consumerIndex == index) {
                RETURN_IF_NOT_OK(client->Subscribe(streamName, subsConfig, consumer));
            }
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, prodConf));
            producers.emplace_back(std::move(producer));
        }
        int threadCount = producers.size() + 1;
        ThreadPool pool(threadCount);
        std::vector<std::future<Status>> futs;
        int index = 0;
        for (auto &producer : producers) {
            futs.push_back(pool.Submit([this, producer, index]() {
                LOG(INFO) << "producer index:" << index;
                size_t dataSize = 600 * 1024;
                auto data = RandomData().GetRandomString(dataSize);
                RETURN_IF_NOT_OK(SendRandomHelper(producer, numElements, data));
                RETURN_IF_NOT_OK(producer->Close());
                return Status::OK();
            }));
            index++;
        }
        size_t recvNumElements = numElements * producers.size();
        futs.push_back(
            pool.Submit([this, consumer, recvNumElements]() { return ReceiveHelper(consumer, recvNumElements); }));
        Status lastRc;
        for (auto &fut : futs) {
            auto rc = fut.get();
            if (rc.IsError()) {
                lastRc = rc;
            }
        }
        return lastRc;
    };

    std::string streamName = "stream-0";
    DS_ASSERT_OK(start(streamName, 0));
}

class SharedPageSendRecvOOMTest : public SharedPageSendRecvTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;
};

void SharedPageSendRecvOOMTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = DEFAULT_WORKER_NUM;
    opts.enableDistributedMaster = "true";
    opts.numRpcThreads = 0;
    opts.vLogLevel = DEFAULT_LOG_LEVEL;
    // Increase the zmq_chunk_sz to beyond shared page size, so then it can batch elements from multiple pages.
    opts.workerGflagParams = " -zmq_chunk_sz=10485760";
    SCClientCommon::SetClusterSetupOptions(opts);
}

TEST_F(SharedPageSendRecvOOMTest, TestSingleStreamOOM)
{
    // Test that a single stream with shared page enabled, requests are retried if necessary upon OOM.
    // OOM is injected to guarantee consistency.
    // Start consumers first for now.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.UsageMonitor.CheckOverUsedForStream.MockError",
                                           "5*return(K_OUT_OF_MEMORY)"));
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(w2Client_->Subscribe("stream1", config, consumer1));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.pageSize = 1 * MB;
    conf.streamMode = StreamMode::SPSC;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream1", producer1, conf));

    const size_t sizeElement = 100 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());

    const int threadNum = 2;
    const size_t numElements = 500;
    ThreadPool pool(threadNum);
    std::vector<std::future<Status>> futs;
    futs.push_back(
        pool.Submit([this, producer1, &element1]() { return SendHelper(producer1, numElements, element1); }));
    futs.push_back(pool.Submit(
        [this, consumer1, &writeElement1]() { return ReceiveHelper(consumer1, numElements, writeElement1); }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, "stream1"));
}
}  // namespace st
}  // namespace datasystem

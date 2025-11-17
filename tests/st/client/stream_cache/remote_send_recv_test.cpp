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
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
#define MULTI_NODE
#ifdef MULTI_NODE
constexpr int K_TWO = 2;
constexpr int K_TEN = 10;
constexpr int K_TWENTY = 20;
class RemoteSendRecvTest : public SCClientCommon {
#else
class RemoteSendRecvMoreTest : public CommonTest {
#endif
public:
#ifdef MULTI_NODE

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;
#endif
    void SetUp() override;

    void TearDown() override;

    static std::string streamName_;
    static std::once_flag onceFlag_;

protected:
    static Status Produce(std::shared_ptr<Producer> &producer, std::string producerName, uint64_t eleSz);

    static Status ConsumeAll(std::shared_ptr<Consumer> &consumer, int timeout = 5000, bool checkFIFO = true,
                             uint64_t *res = nullptr, int producerNum = 1);

    static Status ConsumeAllClose(std::shared_ptr<Consumer> &consumer, int timeout = 5000, bool checkFIFO = true,
                                  uint64_t *res = nullptr, int producerNum = 1);

    Status TestConsumerSetup(const std::string &subName, const std::string &streamName,
                             std::shared_ptr<StreamClient> &client, std::promise<void> &promise,
                             std::shared_ptr<Consumer> &consumer);

    Status TestProducerSetup(std::vector<std::shared_future<void>> &sFuts, std::shared_ptr<StreamClient> &client,
                             const std::string &streamName, const std::string &producerName,
                             std::shared_ptr<Producer> &producer);

    void BothDirectionTestCreateProducers(std::vector<std::future<Status>> &futs, std::shared_ptr<ThreadPool> &pool,
                                          std::vector<std::shared_future<void>> &sFuts, const std::string &streamName,
                                          std::vector<std::shared_ptr<Producer>> &producers);

    void BothDirectionTestCreateConsumers(std::vector<std::future<Status>> &futs, std::shared_ptr<ThreadPool> &pool,
                                          std::vector<std::promise<void>> &promises, const std::string &streamName);

    Status CheckFuts(std::vector<std::future<Status>> &futs);

    // Different tests for a given stream name.
    void SingleThreaded(int round, uint64_t num_eles, ProducerConf producerConf);

    void BasicSPSC(int round, bool checkFIFO = true, uint64_t eleSz = 2 * KB);

    void SendSideMultiProducers(int round);

    void RecvSideAddConsumer(int round);

    void SendSideConsumer(int round);

    void BothDirection(int round);

    void CreateStream_different_client(std::string base_data, int pagesize, std::string name, int send_num);

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
};
std::string RemoteSendRecvTest::streamName_ = "stream";
std::once_flag RemoteSendRecvTest::onceFlag_;

#ifdef MULTI_NODE
void RemoteSendRecvTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = 3;
    opts.enableDistributedMaster = "false";
    opts.workerGflagParams = " -page_size=" + std::to_string(PAGE_SIZE);
    opts.numRpcThreads = 0;
    opts.vLogLevel = 2;
    SCClientCommon::SetClusterSetupOptions(opts);
}
#endif

void RemoteSendRecvTest::SetUp()
{
#ifdef MULTI_NODE
    ExternalClusterTest::SetUp();
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, w3Addr_));
#else
    w1Addr_ = HostPort("127.0.0.1", 2295);
    w3Addr_ = HostPort("127.0.0.1", 8666);
    w2Addr_ = HostPort("127.0.0.1", 11589);
#endif
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    // Worker 1.
    InitStreamClient(0, w1Client_);
    InitStreamClient(1, w2Client_);
    InitStreamClient(2, w3Client_);
    defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
}

void RemoteSendRecvTest::TearDown()
{
    w1Client_ = nullptr;
    w2Client_ = nullptr;
    w3Client_ = nullptr;
#ifdef MULTI_NODE
    ExternalClusterTest::TearDown();
#endif
}

Status RemoteSendRecvTest::Produce(std::shared_ptr<Producer> &producer, std::string producerName, uint64_t eleSz)
{
    Status stat;
    ElementGenerator elementGenerator(eleSz);
    auto strs = elementGenerator.GenElements(producerName, NUM_ELES, 1);

    for (int i = 0; i < NUM_ELES; i++) {
        RETURN_IF_NOT_OK(producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size())));
    }
    return producer->Close();
}

Status RemoteSendRecvTest::ConsumeAll(std::shared_ptr<Consumer> &consumer, int timeout, bool checkFIFO, uint64_t *res,
                                      int producerNum)
{
    std::vector<Element> outElements;
    size_t expectNum = NUM_ELES * producerNum;
    RETURN_IF_NOT_OK(consumer->Receive(expectNum, timeout, outElements));
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

Status RemoteSendRecvTest::ConsumeAllClose(std::shared_ptr<Consumer> &consumer, int timeout, bool checkFIFO,
                                           uint64_t *res, int producerNum)
{
    RETURN_IF_NOT_OK(ConsumeAll(consumer, timeout, checkFIFO, res, producerNum));
    RETURN_IF_NOT_OK(consumer->Close());
    return Status::OK();
}

void RemoteSendRecvTest::SingleThreaded(int round, uint64_t num_eles, ProducerConf producerConf)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    std::shared_ptr<Consumer> w2consumer;
    std::shared_ptr<Consumer> w1Consumer;
    std::shared_ptr<Producer> w1Producer;
    // Worker 1 pub/subs.
    ASSERT_EQ(w1Client_->CreateProducer(streamName, w1Producer, producerConf), Status::OK());
    SubscriptionConfig localConfig("sub2", SubscriptionType::STREAM);
    ASSERT_EQ(w1Client_->Subscribe(streamName, localConfig, w1Consumer), Status::OK());

    // Worker 2 subs.
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    ASSERT_EQ(w2Client_->Subscribe(streamName, config, w2consumer), Status::OK());

    ElementGenerator elementGenerator(BIG_SIZE / 4);
    std::string producerName = "producer1";
    auto strs = elementGenerator.GenElements(producerName, num_eles);

    // Send.
    for (uint8_t i = 0; i < num_eles; i++) {
        ASSERT_EQ(w1Producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size())), Status::OK());
    }

    std::vector<Element> outElements1;
    outElements1.reserve(num_eles);
    std::vector<Element> outElements2;
    outElements2.reserve(num_eles);
    bool c1Finish = false;
    bool c2Finish = false;
    // Recv.
    auto begin = Timer();
    int timeoutMs = 30 * 1000;
    while (begin.ElapsedMilliSecond() <= timeoutMs) {
        if (!c1Finish) {
            std::vector<Element> output1;
            w1Consumer->Receive(5, 0, output1);
            outElements1.insert(outElements1.end(), output1.begin(), output1.end());
            c1Finish = (outElements1.size() == num_eles);
        }
        if (!c2Finish) {
            std::vector<Element> output2;
            w2consumer->Receive(5, 0, output2);
            outElements2.insert(outElements2.end(), output2.begin(), output2.end());
            c2Finish = (outElements2.size() == num_eles);
        }
        if (c1Finish && c2Finish) {
            break;
        }
    }

    ASSERT_EQ(w1Consumer->Ack(num_eles), Status::OK());
    ASSERT_EQ(w1Producer->Close(), Status::OK());
    ASSERT_EQ(w2consumer->Ack(num_eles), Status::OK());
    ASSERT_EQ(w1Consumer->Close(), Status::OK());
    ASSERT_EQ(w2consumer->Close(), Status::OK());
}

// W1: Producer, Consumer. (Each five elements flush once).
// W2: Consumer.
TEST_F(RemoteSendRecvTest, TestSingleThreaded)
{
    auto rounds = 5;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Execute([this, round]() { SingleThreaded(round, NUM_ELES, defaultProducerConf_); });
    }
}

TEST_F(RemoteSendRecvTest, TestSingleThreadedWindow)
{
    // Sets window size to 4 and sends four at a time
    ProducerConf producerConf;
    producerConf.pageSize = TEST_STREAM_SIZE / 4;
    producerConf.maxStreamSize = TEST_STREAM_SIZE;

    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Execute([this, round, producerConf]() { SingleThreaded(round, NUM_ELES, producerConf); });
    }
}

TEST_F(RemoteSendRecvTest, TestSingleThreadedWindowHalf)
{
    // Sets the stream size to half of available shm memory
    ProducerConf producerConf;
    producerConf.pageSize = TEST_STREAM_SIZE/4;
    producerConf.maxStreamSize = TEST_STREAM_SIZE/2;

    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Execute([this, round, producerConf]() { SingleThreaded(round, NUM_ELES, producerConf); });
    }
}

void RemoteSendRecvTest::BasicSPSC(int round, bool checkFIFO, uint64_t eleSz)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    ThreadPool pool(10);
    for (int i = 0; i < 1; i++) {
        LOG(INFO) << FormatString("===================== [Round: %d] [%s] Start =====================", i, streamName);
        std::vector<std::future<Status>> futs;
        std::promise<void> promise;
        // w1:p1
        futs.emplace_back(pool.Submit([this, &promise, streamName, eleSz]() {
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
            promise.get_future().get();

            // Send.
            RETURN_IF_NOT_OK(Produce(producer, "producer1", eleSz));
            return Status::OK();
        }));
        // w2:c1
        futs.emplace_back(pool.Submit([this, &promise, streamName, checkFIFO]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            promise.set_value();

            RETURN_IF_NOT_OK(ConsumeAllClose(consumer, 10'000, checkFIFO));
            return Status::OK();
        }));
        DS_ASSERT_OK(CheckFuts(futs));
        LOG(INFO) << FormatString("Finish: %d", i);
        LOG(INFO) << FormatString("===================== [Round: %d] [%s] End =====================", i, streamName);
    }
    ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
}

// Recv slower than send.
// W1: Producer.
// W2: Consumer.
TEST_F(RemoteSendRecvTest, DISABLED_TestBasicSPSC)
{
    auto rounds = 1;
    ThreadPool roundThreads(rounds);
    for (int round = 0; round < rounds; round++) {
        roundThreads.Execute([this, round]() { BasicSPSC(round); });
    }
}

TEST_F(RemoteSendRecvTest, TestParallelRemoteSendProfiling)
{
    auto rounds = 8;
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(rounds));

    for (int round = 0; round < rounds; round++) {
        roundThreads->Execute([this, round]() { BasicSPSC(round, false, 64); });
    }
}

void RemoteSendRecvTest::SendSideMultiProducers(int round)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    std::unique_ptr<ThreadPool> pool;
    int poolSize = 10;
    LOG_IF_EXCEPTION_OCCURS(pool = std::make_unique<ThreadPool>(poolSize));
    std::stringstream ss;
    int okCnt = 0;
    std::vector<std::string> ids;
    ids.resize(15);
    for (int i = 0; i < 5; i++) {
        std::vector<std::future<Status>> futs;
        std::promise<void> promise;
        std::shared_future<void> sFut = promise.get_future();
        for (auto j = 0; j < 2; j++) {
            futs.emplace_back(pool->Submit([this, &sFut, i, j, streamName, &ids]() {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
                sFut.get();
                auto producerId = "producer" + std::to_string(i * 5 + j);
                ids[i * 3 + j] = producerId;

                // Send.
                RETURN_IF_NOT_OK(Produce(producer, producerId, 2 * KB));
                return Status::OK();
            }));
        }
        futs.emplace_back(pool->Submit([this, &promise, streamName, i, &ids]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub1", SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
            auto consumerId = "consumer" + std::to_string(i);
            ids[i * 3 + 2] = consumerId;
            promise.set_value();

            return ConsumeAllClose(consumer, 20'000, false, nullptr, 2);
        }));
        int index = 0;
        for (auto &fut : futs) {
            auto status = fut.get();
            ss << ((index % 3 == 2) ? "Consumer: " : "Producer:") << ids[index];
            index++;
            if (status.IsError()) {
                ss << "----------------------" << status.ToString();
            } else {
                okCnt++;
                ss << "------------OK------------";
            }
            ss << std::endl;
        }
        LOG(INFO) << FormatString("Finish-iteration: %d", i);
    }
    // Delete stream in last round
    ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
    LOG(INFO) << "ok:" << okCnt << ", status:" << ss.str();
    EXPECT_EQ(okCnt, 15);
}

// W1: Two producers.
// W2: Consumer.
// Flush need FIFO for a producer.
TEST_F(RemoteSendRecvTest, TestSendSideMultiProducers)
{
    auto rounds = 2;
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(rounds));
    for (int round = 0; round < rounds; round++) {
        roundThreads->Execute([this, round]() { SendSideMultiProducers(round); });
    }
}

void RemoteSendRecvTest::RecvSideAddConsumer(int round)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    std::unique_ptr<ThreadPool> pool;
    LOG_IF_EXCEPTION_OCCURS(pool = std::make_unique<ThreadPool>(10));
    for (int i = 0; i < 10; i++) {
        std::vector<std::future<Status>> futs;
        std::future<Status> futs1;
        std::promise<void> promise;
        std::shared_future<void> sfut = promise.get_future();

        futs.emplace_back(pool->Submit([this, &sfut, streamName]() {
            std::shared_ptr<Producer> producer;
            std::vector<std::shared_future<void>> sfuts = { sfut };
            return TestProducerSetup(sfuts, w1Client_, streamName, "producer", producer);
        }));
        futs1 = pool->Submit([this, &promise, streamName]() {
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(TestConsumerSetup("sub1", streamName, w2Client_, promise, consumer));
            return ConsumeAllClose(consumer);
        });
        futs.emplace_back(pool->Submit([this, &futs1, &sfut, streamName]() {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub2", SubscriptionType::STREAM);
            sfut.get();
            RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));

            std::vector<Element> outElements;
            std::unordered_map<std::string, uint64_t> seqNoMap;
            int recvNum = 0;
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
                consumer->Ack(recvNum);
            }
            CHECK_FAIL_RETURN_STATUS(recvNum <= NUM_ELES, StatusCode::K_RUNTIME_ERROR, "");
            return consumer->Close();
        }));
        for (auto &fut : futs) {
            fut.get();
        }
        futs1.get();
        LOG(INFO) << FormatString("Finish: %d", i);
    }
    ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
}

// W1: Producer.
// W: Consumer, then dynamically add another.
TEST_F(RemoteSendRecvTest, DISABLED_TestRecvSideAddConsumer)
{
    auto rounds = 5;
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(rounds));
    for (int round = 0; round < rounds; round++) {
        roundThreads->Execute([this, round]() { RecvSideAddConsumer(round); });
    }
}

Status RemoteSendRecvTest::TestConsumerSetup(const std::string &subName, const std::string &streamName,
                                             std::shared_ptr<StreamClient> &client, std::promise<void> &promise,
                                             std::shared_ptr<Consumer> &consumer)
{
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    RETURN_IF_NOT_OK(client->Subscribe(streamName, config, consumer));
    promise.set_value();
    return Status::OK();
}

Status RemoteSendRecvTest::TestProducerSetup(std::vector<std::shared_future<void>> &sFuts,
                                             std::shared_ptr<StreamClient> &client, const std::string &streamName,
                                             const std::string &producerName, std::shared_ptr<Producer> &producer)
{
    RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, defaultProducerConf_));

    // Send.
    for (auto &sFut : sFuts) {
        sFut.get();
    }
    return Produce(producer, producerName, 10);
}

Status RemoteSendRecvTest::CheckFuts(std::vector<std::future<Status>> &futs)
{
    for (auto &fut : futs) {
        RETURN_IF_NOT_OK(fut.get());
    }
    return Status::OK();
}

void RemoteSendRecvTest::SendSideConsumer(int round)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    std::unique_ptr<ThreadPool> pool;
    LOG_IF_EXCEPTION_OCCURS(pool = std::make_unique<ThreadPool>(10));
    for (int i = 0; i < 10; i++) {
        std::vector<std::future<Status>> futs;
        std::vector<std::promise<void>> promises(3);
        std::vector<std::shared_future<void>> sFuts;
        for (auto &promise : promises) {
            sFuts.emplace_back(promise.get_future());
        }
        futs.emplace_back(pool->Submit([this, &promises, streamName]() {
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(TestConsumerSetup("sub1", streamName, w1Client_, promises[0], consumer));

            return ConsumeAllClose(consumer);
        }));
        futs.emplace_back(pool->Submit([this, &promises, streamName]() {
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(TestConsumerSetup("sub2", streamName, w1Client_, promises[1], consumer));

            Timer timer;
            uint64_t sz;
            RETURN_IF_NOT_OK(ConsumeAllClose(consumer, 5'000, true, &sz));
            auto elapsed = timer.ElapsedSecond();
            LOG(INFO) << FormatString("Elapsed: [%.6lf]s, Throughput: [%.6lf] MB/s", elapsed,
                                      sz / timer.ElapsedSecond() / MB);
            return Status::OK();
        }));
        futs.emplace_back(pool->Submit([this, &sFuts, streamName]() {
            std::shared_ptr<Producer> producer;
            return TestProducerSetup(sFuts, w1Client_, streamName, "producer", producer);
        }));
        futs.emplace_back(pool->Submit([this, &promises, streamName]() {
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(TestConsumerSetup("sub3", streamName, w2Client_, promises[2], consumer));
            return ConsumeAllClose(consumer);
        }));
        DS_ASSERT_OK(CheckFuts(futs));
        if (i == 9) {  // Delete stream in last round
            std::this_thread::sleep_for(std::chrono::seconds(2));
            ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
        }
        LOG(INFO) << FormatString("Stream:%d, Finish: %d", round, i);
    }
}

// W1: Producer, Consumer.
// W2: Consumer.
TEST_F(RemoteSendRecvTest, DISABLED_TestSendSideConsumer)
{
    auto rounds = 5;
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(rounds));
    for (int round = 0; round < rounds; round++) {
        roundThreads->Execute([this, round]() { SendSideConsumer(round); });
    }
}

void RemoteSendRecvTest::CreateStream_different_client(std::string base_data, int pagesize, std::string name,
                                                       int send_num) {
    for (int m = 0; m < 1000; m++) {
        std::shared_ptr<StreamClient> client, client2;
        InitStreamClient(0, client);
        InitStreamClient(0, client2);

        ProducerConf conf;
        conf.maxStreamSize = 7 * MB;
        conf.pageSize = pagesize * KB;
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        std::string streamName = "Stream_" + RandomData().GetRandomString(12);
        std::string subName = "Sub_" + RandomData().GetRandomString(13);
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));
        DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

        for (int i = 0; i < send_num; i++) {
            std::string data = base_data + RandomData().GetRandomString(13);
            Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
            DS_ASSERT_OK(producer->Send(element));
            std::vector<Element> outElements;
            DS_ASSERT_OK(consumer->Receive(1, 10000, outElements));
            if (outElements.size() == 1) {
                std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
                DS_ASSERT_OK(consumer->Ack(outElements[0].id));
            } else {
                LOG(INFO) << name << " outElements.size() is 0";
            }
        }
        DS_ASSERT_OK(producer->Close());
        DS_ASSERT_OK(consumer->Close());
        DS_ASSERT_OK(client->DeleteStream(streamName));

        DS_ASSERT_OK(client->ShutDown());
        DS_ASSERT_OK(client2->ShutDown());
    }
}

TEST_F(RemoteSendRecvTest, DISABLED_TestMemoryAllocationOverflow)
{
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(10));
    std::string base_data = RandomData().GetRandomString(1020 * 1020);
    roundThreads->Execute([this, base_data]() {
        CreateStream_different_client(base_data, 1024, "continuous_data", 5);
    });
}

void RemoteSendRecvTest::BothDirectionTestCreateProducers(std::vector<std::future<Status>> &futs,
                                                          std::shared_ptr<ThreadPool> &pool,
                                                          std::vector<std::shared_future<void>> &sFuts,
                                                          const std::string &streamName,
                                                          std::vector<std::shared_ptr<Producer>> &producers)
{
    futs.emplace_back(pool->Submit([this, &sFuts, streamName, &producers]() {
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(TestProducerSetup(sFuts, w1Client_, streamName, "producer1", producer));
        producers.emplace_back(std::move(producer));
        return Status::OK();
    }));
    futs.emplace_back(pool->Submit([this, &sFuts, streamName, &producers]() {
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(TestProducerSetup(sFuts, w2Client_, streamName, "producer2", producer));
        producers.emplace_back(std::move(producer));
        return Status::OK();
    }));
    futs.emplace_back(pool->Submit([this, &sFuts, streamName, &producers]() {
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(TestProducerSetup(sFuts, w3Client_, streamName, "producer3", producer));
        producers.emplace_back(std::move(producer));
        return Status::OK();
    }));
}

void RemoteSendRecvTest::BothDirectionTestCreateConsumers(std::vector<std::future<Status>> &futs,
                                                          std::shared_ptr<ThreadPool> &pool,
                                                          std::vector<std::promise<void>> &promises,
                                                          const std::string &streamName)
{
    futs.emplace_back(pool->Submit([this, &promises, streamName]() {
        std::shared_ptr<Consumer> consumer;
        RETURN_IF_NOT_OK(TestConsumerSetup("sub1", streamName, w1Client_, promises[0], consumer));
        return ConsumeAllClose(consumer, 2000);
    }));
    futs.emplace_back(pool->Submit([this, &promises, streamName]() {
        std::shared_ptr<Consumer> consumer;
        RETURN_IF_NOT_OK(TestConsumerSetup("sub2", streamName, w1Client_, promises[1], consumer));
        return ConsumeAllClose(consumer, 2000);
    }));
    futs.emplace_back(pool->Submit([this, &promises, streamName]() {
        std::shared_ptr<Consumer> consumer;
        RETURN_IF_NOT_OK(TestConsumerSetup("sub3", streamName, w2Client_, promises[2], consumer));
        return ConsumeAllClose(consumer, 2000);
    }));
}

void RemoteSendRecvTest::BothDirection(int round)
{
    auto streamName = FormatString("%s-%d", streamName_, round);
    std::shared_ptr<ThreadPool> pool;
    LOG_IF_EXCEPTION_OCCURS(pool = std::make_shared<ThreadPool>(10));
    for (int i = 0; i < 10; i++) {
        LOG(INFO) << FormatString("===================== [Round: %d] Start =====================", i);
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::future<Status>> futs;
        std::vector<std::promise<void>> promises(3);
        std::vector<std::shared_future<void>> sFuts;
        for (auto &promise : promises) {
            sFuts.emplace_back(promise.get_future());
        }
        // create producers
        BothDirectionTestCreateProducers(futs, pool, sFuts, streamName, producers);
        // create consumers
        BothDirectionTestCreateConsumers(futs, pool, promises, streamName);
        DS_ASSERT_OK(CheckFuts(futs));
        for (auto &producer : producers) {
            ASSERT_EQ(producer->Close(), Status::OK());
        }
        if (i == 9) {  // Delete stream in last round
            std::this_thread::sleep_for(std::chrono::seconds(2));
            ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
        }
        LOG(INFO) << FormatString("Finish: %d", i);
        LOG(INFO) << FormatString("===================== [Round: %d] End =====================", i);
    }
}

// W1: Producer, 2Consumer.
// W2: Producer, Consumer.
// W3: Producer.
TEST_F(RemoteSendRecvTest, DISABLED_TestBothDirection)
{
    auto rounds = 5;
    std::unique_ptr<ThreadPool> roundThreads;
    LOG_IF_EXCEPTION_OCCURS(roundThreads = std::make_unique<ThreadPool>(rounds));
    for (int round = 0; round < rounds; round++) {
        roundThreads->Execute([this, round]() { BothDirection(round); });
    }
}

TEST_F(RemoteSendRecvTest, TestProducerCloseAck)
{
    std::shared_ptr<Producer> producer;
    auto streamName = "stream-test-producer";
    w1Client_->CreateProducer(streamName, producer, defaultProducerConf_);
    // Send.
    Produce(producer, FormatString("producer-stream-test"), 2 * KB);
    producer->Close();
    ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
}

TEST_F(RemoteSendRecvTest, TestAutoDeleteWaitFailed)
{
    // Ack thread will get protect delete on stream manager and sleeps for 2secs
    // If we get DeleteStreamContext during this time, we will end up waiting for StreamManager to be free
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamManager.AckCursors.delay", "2*sleep(2000)"));
    // We set timeout to be 1secs, so we fail the wait in DeleteStreamContext once
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
                                           "MasterLocalWorkerSCApi.DelStreamContextBroadcast.setTimeout",
                                           "call(1000)"));
    std::string streamName = "streamAutoDelWaitFailed";
    defaultProducerConf_.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    DS_ASSERT_OK(producer->Close());
    // Auto delete retries every 10secs
    // and then test if stream is deleted
    sleep(10);
    // Now stream should have been deleted
    ASSERT_EQ(w1Client_->DeleteStream(streamName).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(RemoteSendRecvTest, DISABLED_TestAutoDeleteDeadlock)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamManager.AckCursors.delay", "1*sleep(100000)"));
    std::string streamName = "stream";
    ThreadPool threadPool(7);
    std::vector<std::future<void>> futs;
    defaultProducerConf_.autoCleanup = true;
    for (u_int i = 0; i < 7; i++) {
        auto fut = threadPool.Submit([&]() {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
            for (uint32_t j = 0; j < 100000; j++) {
                std::string data = "data_" + std::to_string(i);
                Element element((uint8_t *)data.data(), data.size());
                DS_ASSERT_OK(producer->Send(element));
            }
            DS_ASSERT_OK(producer->Close());
        });
        futs.push_back(std::move(fut));
    }

    for (auto &fut : futs) {
        fut.get();
    }
}

TEST_F(RemoteSendRecvTest, TestRemotePushToMultiNode)
{
    std::string streamName = "streamRemotePushMultiNode";
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName, config1, consumer1));
    DS_ASSERT_OK(w3Client_->Subscribe(streamName, config2, consumer2));

    const uint32_t testCount = 100;
    const uint32_t waitTime = 3000;  // 3s.
    for (uint32_t i = 0; i < testCount; i++) {
        std::string data = "data_" + std::to_string(i);
        Element element((uint8_t *)data.data(), data.size());
        DS_ASSERT_OK(producer->Send(element));
    }
    std::vector<Element> outElements1;
    DS_ASSERT_OK(consumer1->Receive(testCount, waitTime, outElements1));
    EXPECT_EQ(outElements1.size(), testCount);

    std::vector<Element> outElements2;
    DS_ASSERT_OK(consumer2->Receive(testCount, waitTime, outElements2));
    EXPECT_EQ(outElements2.size(), testCount);
}

void SendHelper(std::shared_ptr<Producer> producer, Element element)
{
    const int DEFAULT_SLEEP_TIME = 300;
    int retryLimit = 30;
    datasystem::Status rc = producer->Send(element);
    if (rc.IsError()) {
        while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
            rc = producer->Send(element);
        }
    }
    DS_ASSERT_OK(rc);
}

TEST_F(RemoteSendRecvTest, TestRemotePushToMultiNodeDifferentOrder)
{
    // Create a producer and consumer for a stream on Node1 and Node2
    // Having a consumer for any stream will prevent remoteWorker from getting deleted
    std::string streamName1 = "testMultiNodeDiffOrder_s1";
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName1, producer1, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName1, config1, consumer1));

    // Create a new stream for our actual test
    std::string streamName2 = "testMultiNodeDiffOrder_s2";
    defaultProducerConf_.maxStreamSize = 2*1024*1024;
    defaultProducerConf_.autoCleanup = true;
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName2, producer2, defaultProducerConf_));

    // Create a consumer on same node
    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName2, config2, consumer2));

    // Close producer and consumer
    producer2->Close();
    consumer2->Close();
    sleep(K_TEN); // wait for AutoDelete

    // Now Repeat above steps
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName2, producer3, defaultProducerConf_));

    // Create a consumer on different node
    // Now worker will try to use AckCursor from previous RemoteWorker
    std::shared_ptr<Consumer> consumer3;
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    DS_ASSERT_OK(w3Client_->Subscribe(streamName2, config3, consumer3));

    const uint32_t testCount = 1000;
    const uint32_t eleSz = 2*1024;
    const uint32_t waitTime = 3000;  // 3s.
    ElementGenerator elementGenerator(eleSz + 1, eleSz);
    auto elements = elementGenerator.GenElements("producer", testCount, 8ul);
    ThreadPool threadPool(1);
    uint64_t numElements = 0;
    auto fut = threadPool.Submit([&]() {
        while (numElements < testCount) {
            std::vector<Element> outElements2;
            DS_ASSERT_OK(consumer3->Receive(100, waitTime, outElements2));
            if (!outElements2.empty()) {
                numElements += outElements2.size();
                LOG(INFO) << "Got num Elements: "<<numElements<<" Acking id: "<<outElements2.back().id;
                DS_ASSERT_OK(consumer3->Ack(outElements2.back().id));
            }
        }
    });
    elements = elementGenerator.GenElements("producer", testCount, 8ul);
    for (auto ele : elements) {
        SendHelper(producer3, Element((uint8_t *)(ele.data()), ele.size()));
    }
    fut.get();
    // After receive is done, check if we got enough count
    EXPECT_EQ(numElements, testCount);
}

TEST_F(RemoteSendRecvTest, DISABLED_TestRemoteSendTimeout)
{
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.delayFlushTime = 1000;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    DS_ASSERT_OK(w1Client_->CreateProducer("stream", producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe("stream", config, consumer));

    std::string str("abcabc");
    Element e((uint8_t *)str.data(), str.length());
    DS_ASSERT_OK(producer->Send(e));

    const int receiveTimeout = 1000;  // 1s;

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, receiveTimeout, outElements));
    ASSERT_EQ(outElements.size(), 1ul);

    const int remoteReceiveTimeout = 10000;  // 10s;
    std::this_thread::sleep_for(std::chrono::milliseconds(remoteReceiveTimeout));

    const size_t loopCount = 3;
    for (size_t i = 0; i < loopCount; i++) {
        DS_ASSERT_OK(producer->Send(e));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    DS_ASSERT_OK(consumer->Receive(loopCount, receiveTimeout, outElements));
    ASSERT_EQ(outElements.size(), loopCount);
}

TEST_F(RemoteSendRecvTest, TestFlowControl)
{
    std::string streamName = "streamFlowCtrl";
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = 16 * 1024;
    conf.maxStreamSize = 64 * 1024;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
    const uint32_t eleSz = 512;
    const uint32_t eleNum = 200;
    ElementGenerator elementGenerator(eleSz);
    auto strs = elementGenerator.GenElements("producer1", eleNum, 1);
    const int64_t timeoutMs = 1000;
    for (uint32_t i = 0; i < eleNum; i++) {
        DS_ASSERT_OK(producer->Send(Element((uint8_t *)strs[i].data(), strs[i].size()), timeoutMs));
    }
    const uint32_t recvNum = 100;
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(recvNum, 5000, outElements));
    ASSERT_EQ(outElements.size(), recvNum);
    // Do not ack for some time so then the max stream size is reached on Consumer side worker
    std::this_thread::sleep_for(std::chrono::seconds(5));
    DS_ASSERT_OK(consumer->Ack(recvNum));

    // Retry receive upon failure for testcase stability purposes
    uint32_t remaining = recvNum;
    int retryCount = 3;
    while (remaining > 0 && retryCount-- > 0) {
        DS_ASSERT_OK(consumer->Receive(remaining, 5000, outElements));
        uint32_t received = outElements.size();
        DS_ASSERT_OK(consumer->Ack(received));
        remaining -= received;
    }
    ASSERT_EQ(remaining, 0);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(TryAndDeleteStream(w1Client_, streamName));
}

class RemoteSendRecvBigElementTest : public RemoteSendRecvTest
{
public:
    const int minThreads = 20;
    const int maxThreads = 128;
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        RemoteSendRecvTest::SetClusterSetupOptions(opts);
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            "-shared_memory_size_mb=512 -client_dead_timeout_s=15 -enable_stream_data_verification=true";
        opts.vLogLevel = SC_INTERNAL_LOG_LEVEL;
    }
    void SetUp() override
    {
        RemoteSendRecvTest::SetUp();
        const int numPages = 16;
        defaultProducerConf_.pageSize = 1 * MB;
        defaultProducerConf_.maxStreamSize = defaultProducerConf_.pageSize * numPages;
        defaultProducerConf_.retainForNumConsumers = 1;
        pool = std::make_unique<ThreadPool>(minThreads, maxThreads);
        allClients_.push_back(w1Client_.get());
        allClients_.push_back(w2Client_.get());
        allClients_.push_back(w3Client_.get());
    }
    void TearDown() override
    {
        RemoteSendRecvTest::TearDown();
    }

protected:
    std::unique_ptr<ThreadPool> pool;
    std::vector<StreamClient *> allClients_;

    Status FillMemoryUntilOOM(const std::string &streamName, size_t numProducersPerWorker, size_t minEleSz,
                              size_t maxEleSz, size_t &totalElements)
    {
        std::atomic<size_t> numInserted = 0;
        std::vector<std::future<Status>> fut;
        for (auto *client : allClients_) {
            for (size_t i = 0; i < numProducersPerWorker; ++i) {
                fut.emplace_back(
                    pool->Submit([this, client, &streamName, &numInserted, &minEleSz, &maxEleSz]() -> Status {
                        RandomData rand;
                        std::shared_ptr<Producer> producer;
                        RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, defaultProducerConf_));
                        // Send small elements until EOM
                        Status rc;
                        while (rc.IsOk()) {
                            auto eleSz = rand.GetRandomUint64(minEleSz, maxEleSz + 1);
                            auto str = rand.GetRandomString(eleSz);
                            rc = producer->Send(Element((uint8_t *)str.data(), str.size()));
                            if (rc.IsOk()) {
                                numInserted++;
                            }
                        }
                        return rc;
                    }));
            }
        }
        for (auto &f : fut) {
            auto res = f.get();
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res.GetCode() == StatusCode::K_OUT_OF_MEMORY, K_RUNTIME_ERROR,
                                                 FormatString("Expect OOM but get %s", res.ToString()));
        }
        totalElements = numInserted.load(std::memory_order_relaxed);
        LOG(INFO) << "Total number of small elements insert: " << totalElements;
        return Status::OK();
    }

    void CreateProducersAndPush(std::vector<std::future<Status>> &fut, const std::string &streamName,
                                int initialNumOfProducersPerWorker, size_t minEleSz, size_t maxEleSz,
                                size_t totalElements)
    {
        for (auto *client : allClients_) {
            for (auto i = 0; i < initialNumOfProducersPerWorker; ++i) {
                fut.emplace_back(pool->Submit([this, client, streamName, totalElements, minEleSz,
                                               maxEleSz]() -> Status {
                    std::shared_ptr<Producer> producer;
                    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(client->CreateProducer(streamName, producer, defaultProducerConf_),
                                                     FormatString("[S:%s] CreateProducer failed.", streamName));
                    LOG(INFO) << FormatString("[S:%s] CreateProducer success. Number of elements to push %zu",
                                              streamName, totalElements);
                    size_t numElementSent = 0;
                    Status rc;
                    const int bigEleSz = 2 * MB;
                    RandomData rand;
                    const int FREQUENCY = 1000;  // 0.1% will be big element
                    while (numElementSent < totalElements) {
                        auto eleSz = rand.GetRandomUint64(minEleSz, maxEleSz + 1);
                        size_t sz = (numElementSent > 0 && numElementSent % FREQUENCY == 0) ? bigEleSz : eleSz;
                        auto str = rand.GetRandomString(sz);
                        rc = producer->Send(Element((uint8_t *)str.data(), str.size()));
                        if (rc.IsOk()) {
                            numElementSent++;
                            continue;
                        }
                        // rest is error case
                        LOG(ERROR) << FormatString("[S:%s] Fail to send. rc = %s", streamName, rc.ToString());
                        if (rc.GetCode() == K_OUT_OF_MEMORY) {
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            continue;
                        }
                        break;
                    }
                    LOG(INFO) << FormatString("[S:%s] %zu number of elements pushed", streamName, numElementSent);
                    return rc;
                }));
            }
        }
    }

    void ConsumeAndAckAll(std::vector<std::future<Status>> &fut, const std::string &streamName,
                          const size_t totalElements, Optional<RandomData> rand)
    {
        int idx = allClients_.size() - 1;
        if (rand) {
            idx = rand.value().GetRandomIndex(allClients_.size());
        }
        auto *streamClient = allClients_.at(idx);
        LOG(INFO) << FormatString("[S:%s] Create consumer on worker node %d", streamName, idx);
        fut.emplace_back(pool->Submit([streamName, totalElements, streamClient]() -> Status {
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig localConfig(streamName + "_sub000", SubscriptionType::STREAM);
            RETURN_IF_NOT_OK(streamClient->Subscribe(streamName, localConfig, consumer, true));
            LOG(INFO) << FormatString("[S:%s] Total elements expected to receive: %zu", streamName, totalElements);
            size_t numElementsReceived = 0;
            while (numElementsReceived < totalElements) {
                std::vector<Element> out;
                RETURN_IF_NOT_OK(consumer->Receive(RPC_TIMEOUT, out));
                if (!out.empty()) {
                    numElementsReceived += out.size();
                    LOG(INFO) << FormatString("[S:%s] Received %zu. Remaining %zu", streamName, numElementsReceived,
                                              totalElements - numElementsReceived);
                    consumer->Ack(out[out.size() - 1].id);
                }
            }
            return Status::OK();
        }));
    }

    void SendOneBigElement(std::vector<std::future<Status>> &fut, const std::string &streamName)
    {
        for (auto *client : allClients_) {
            fut.emplace_back(pool->Submit([this, client, streamName]() -> Status {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, defaultProducerConf_));
                // Send a big element
                const int bigEleSz = 4 * MB;
                RandomData rand;
                auto str = rand.GetRandomString(bigEleSz);
                Status rc = producer->Send(Element((uint8_t *)str.data(), str.size()), RPC_TIMEOUT);
                return rc;
            }));
        }
    }
};

TEST_F(RemoteSendRecvBigElementTest, TestBigElementFairness1)
{
    const std::string streamName = "BigElementFairness1";
    const int initialNumOfProducersPerWorker = 3;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 0;

    // Set up OOM on all the workers without any consumer to consume
    DS_ASSERT_OK(FillMemoryUntilOOM(streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements));

    // Now we create three BigElement producer, and specify a timeout, and then finally
    // create a consumer.
    std::vector<std::future<Status>> futs1;
    SendOneBigElement(futs1, streamName);
    totalElements += allClients_.size();
    ConsumeAndAckAll(futs1, streamName, totalElements, Optional<RandomData>());

    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
}

TEST_F(RemoteSendRecvBigElementTest, TestBigElementFairness2)
{
    const std::string streamName = "BigElementFairness2";
    const int initialNumOfProducersPerWorker = 3;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 0;
    // Set up OOM on all the workers without any consumer to consume
    DS_ASSERT_OK(FillMemoryUntilOOM(streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements));

    // Create a consumer to consume all the rows with 3 additional big element rows which come later
    std::vector<std::future<Status>> futs1;
    totalElements += allClients_.size();
    ConsumeAndAckAll(futs1, streamName, totalElements, Optional<RandomData>());
    SendOneBigElement(futs1, streamName);

    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
}

TEST_F(RemoteSendRecvBigElementTest, DISABLED_TestBigElementFairness3)
{
    // README
    // The numStreams has been decreased from 5 to reduce run time during CI
    // To run the intended load locally, edit the value.
    const int initialNumOfProducersPerWorker = 4;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 12'000;
    const int numStreams = 2;
    const int width = 3;

    std::vector<std::future<Status>> futs1;
    RandomData rand;
    for (int i = 0; i < numStreams; ++i) {
        std::stringstream oss;
        oss << "stream" << std::setw(width) << std::setfill('0') << i;
        const std::string streamName = oss.str();
        LOG(INFO) << "Create stream " << streamName;
        // Create a consumer to consume everything.
        ConsumeAndAckAll(futs1, streamName, totalElements * allClients_.size() * initialNumOfProducersPerWorker,
                         Optional<RandomData>(rand));
        // Create a few producers.
        CreateProducersAndPush(futs1, streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements);
    }

    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
}

TEST_F(RemoteSendRecvBigElementTest, DISABLED_TestBigElementFairness4)
{
    const std::string streamName = "BigElementFairness4";
    const int initialNumOfProducersPerWorker = 3;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 0;
    // Set up OOM on all the workers without any consumer to consume
    DS_ASSERT_OK(FillMemoryUntilOOM(streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements));

    // Create a consumer to consume a few rows and then exit.
    auto pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> scClient;
        InitStreamClient(2, scClient); // index is 2
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig localConfig(streamName + "_sub000", SubscriptionType::STREAM);
        DS_ASSERT_OK(scClient->Subscribe(streamName, localConfig, consumer, true));
        LOG(INFO) << FormatString("[S:%s] Total elements expected to receive: %zu", streamName, totalElements);
        size_t numElementsReceived = 0;
        const size_t exitThreshold = 500;
        while (numElementsReceived < totalElements) {
            std::vector<Element> out;
            if (numElementsReceived >= exitThreshold) {
                break;
            }
            DS_ASSERT_OK(consumer->Receive(RPC_TIMEOUT, out));
            if (!out.empty()) {
                numElementsReceived += out.size();
                LOG(INFO) << FormatString("[S:%s] Received %zu. Remaining %zu", streamName, numElementsReceived,
                                          totalElements - numElementsReceived);
            }
        }
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    int status;
    waitpid(pid, &status, 0);
    // Wait at least client_dead_timeout_s (15s)
    const uint64_t sleepMs = 16'000;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    // Create a consumer to consume 3 additional big element rows which come later
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig localConfig(streamName + "_sub001", SubscriptionType::STREAM);
    DS_ASSERT_OK(w3Client_->Subscribe(streamName, localConfig, consumer, true));
    std::vector<std::future<Status>> futs1;
    totalElements = allClients_.size();
    SendOneBigElement(futs1, streamName);
    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
    std::vector<Element> out;
    DS_ASSERT_OK(consumer->Receive(totalElements, RPC_TIMEOUT, out));
    ASSERT_EQ(out.size(), totalElements);
}

TEST_F(RemoteSendRecvBigElementTest, TestReclaimMemoryReuseStream)
{
    const std::string streamName = "ReclaimMemoryReuseStream";
    const int initialNumOfProducersPerWorker = 2;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 1'000;

    // Create a consumer to consume everything.
    std::vector<std::future<Status>> futs1;
    auto totalCount = totalElements * allClients_.size() * initialNumOfProducersPerWorker;
    ConsumeAndAckAll(futs1, streamName, totalCount, Optional<RandomData>());

    // Create a few producers.
    for (auto *client : allClients_) {
        for (auto i = 0; i < initialNumOfProducersPerWorker; ++i) {
            futs1.emplace_back(pool->Submit([this, client, &streamName, totalElements]() -> Status {
                std::shared_ptr<Producer> producer;
                RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, defaultProducerConf_));
                // 1% will be big elements
                size_t numElementSent = 0;
                Status rc;
                RandomData rand;
                while (numElementSent < totalElements) {
                    // 0.1% will be big element
                    auto eleSz = rand.GetRandomUint64(minEleSz, maxEleSz + 1);
                    auto str = rand.GetRandomString(eleSz);
                    rc = producer->Send(Element((uint8_t *)str.data(), str.size()));
                    if (rc.IsOk()) {
                        numElementSent++;
                    }
                }
                return rc;
            }));
        }
    }

    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }

    futs1.clear();
    // All producers/consumers are closed at this point and all memory are released.
    // Reuse the same stream and supposedly to resume the last ack point.
    futs1.emplace_back(pool->Submit([this, &streamName, totalCount]() {
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig localConfig("sub001", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(w3Client_->Subscribe(streamName, localConfig, consumer, true));
        std::vector<Element> out;
        RETURN_IF_NOT_OK(consumer->Receive(RPC_TIMEOUT, out));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!out.empty(), K_RUNTIME_ERROR, FormatString("Expect not empty"));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            out[0].id == totalCount + 1, K_RUNTIME_ERROR,
            FormatString("Id mismatch. Expect %zu but get %zu", totalCount + 1, out[0].id));
        consumer->Ack(out[0].id);
        return Status::OK();
    }));

    futs1.emplace_back(pool->Submit([this, &streamName]() -> Status {
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
        RandomData rand;
        auto eleSz = rand.GetRandomUint64(minEleSz, maxEleSz + 1);
        auto str = rand.GetRandomString(eleSz);
        RETURN_IF_NOT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));
        return Status::OK();
    }));

    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
}

TEST_F(RemoteSendRecvBigElementTest, TestBlockedReqTimeout1)
{
    const std::string streamName = "TestBlockedReqTimeout1";
    const int initialNumOfProducersPerWorker = 3;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 0;

    // Set up OOM on all the workers without any consumer to consume
    DS_ASSERT_OK(FillMemoryUntilOOM(streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements));
    // Construct a blocked request with timeout 0ms
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AddBlockedCreateRequest.subTimeout", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AutoAckImpl.WaitAndRetry", "2*call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HandleBlockedRequestImpl.subTimeout", "call()"));
    const int FIVE_S = 5;
    std::this_thread::sleep_for(std::chrono::seconds(FIVE_S));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    // Send a big element
    const int bigEleSz = 4 * MB;
    RandomData rand;
    auto str = rand.GetRandomString(bigEleSz);
    Status rc = producer->Send(Element((uint8_t *)str.data(), str.size()), RPC_TIMEOUT);
    LOG(INFO) << rc.ToString();
    DS_ASSERT_NOT_OK(rc);
}

TEST_F(RemoteSendRecvBigElementTest, LEVEL1_TestBlockedReqTimeout2)
{
    const std::string streamName = "BlockedReqTimeout2";
    const int initialNumOfProducersPerWorker = 3;
    const size_t minEleSz = 48;
    const size_t maxEleSz = 1024;
    size_t totalElements = 0;

    // Set up OOM on all the workers without any consumer to consume
    DS_ASSERT_OK(FillMemoryUntilOOM(streamName, initialNumOfProducersPerWorker, minEleSz, maxEleSz, totalElements));
    // Construct a blocked request with timeout 5s
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HandleBlockedRequestImpl.sleep", "1*sleep(10000)"));
    // Send a big element
    const int bigEleSz = 4 * MB;
    RandomData rand;
    auto str = rand.GetRandomString(bigEleSz);
    const int TWO_S = 2000;
    Status rc = producer->Send(Element((uint8_t *)str.data(), str.size()), TWO_S);
    LOG(INFO) << rc.ToString();
    ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
}

TEST_F(RemoteSendRecvBigElementTest, TestBlockedReqTimeout3)
{
    const std::string streamName = "BlockedReqTimeout3";
    const int totalElements = 2;
    const int workerInx = 2;
    // Simulate the case memory is orphaned
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerInx, "StreamManager.AllocBigShmMemoryInternalReq.SetTimeoutMs",
                                           "2*return(0)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, workerInx, "StreamManager.AllocBigShmMemory.NoHandShake1", "call()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, workerInx, "StreamManager.AllocBigShmMemory.NoHandShake2", "return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerInx, "ExclusivePageQueue.Ack.Start", "return(K_OK)"));
    std::vector<std::future<Status>> futs1;
    ConsumeAndAckAll(futs1, streamName, totalElements, Optional<RandomData>());
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Producer> localProducer;
    DS_ASSERT_OK(w3Client_->CreateProducer(streamName, localProducer, defaultProducerConf_));
    const int bigEleSz = 4 * MB;
    RandomData rand;
    auto str = rand.GetRandomString(bigEleSz);
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));
    // The above set up will create some orphaned big element.
    // If we send one more local big element, we will get OOM
    const int FIVE_S = 5000;
    const int K_2 = 2;
    std::this_thread::sleep_for(std::chrono::seconds(K_2));
    Status rc = localProducer->Send(Element((uint8_t *)str.data(), str.size()), FIVE_S);
    LOG(INFO) << rc.ToString();
    ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
    // Need to kill of the consumer which can't get the element sent by the local producer.
    // Easier to send one more dummy elements.
    std::string a("bye");
    localProducer->Send(Element((uint8_t *)a.data(), a.size()));
}

TEST_F(RemoteSendRecvBigElementTest, TestBlockedReqTimeout4)
{
    const std::string streamName = "BlockedReqTimeout4";
    const int totalElements = 1;
    const int workerInx = 2;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerInx, "ExclusivePageQueue.Ack.Start", "return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerInx, "StreamManager.AllocBigShmMemoryInternalReq.sleep",
                                           "1*sleep(5000)"));
    std::vector<std::future<Status>> futs1;
    ConsumeAndAckAll(futs1, streamName, totalElements, Optional<RandomData>());
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    const int bigEleSz = 4 * MB;
    RandomData rand;
    auto str = rand.GetRandomString(bigEleSz);
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));
    for (auto &f : futs1) {
        auto res = f.get();
        DS_ASSERT_OK(res);
    }
}

TEST_F(RemoteSendRecvBigElementTest, TestHandShakeUndo)
{
    const std::string streamName = "testHandShakeUndo";
    const int totalElements = 2;
    const int workerInx = 2;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerInx, "ExclusivePageQueue.Ack.Start", "return(K_OK)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, workerInx, "BlockedCreateRequest.ReceiverHandShake.sleep", "10*sleep(1000)"));
    std::vector<std::future<Status>> futs1;
    ConsumeAndAckAll(futs1, streamName, totalElements, Optional<RandomData>());
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Producer> localProducer;
    DS_ASSERT_OK(w3Client_->CreateProducer(streamName, localProducer, defaultProducerConf_));
    const int bigEleSz = 4 * MB;
    RandomData rand;
    auto str = rand.GetRandomString(bigEleSz);
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));
    const int K_10 = 10000;
    std::this_thread::sleep_for(std::chrono::milliseconds(K_10));
    DS_ASSERT_OK(localProducer->Send(Element((uint8_t *)str.data(), str.size())));
}

class RemoteSendRecvDuplicateTest : public RemoteSendRecvTest {
public:

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = " -sc_local_cache_memory_size_mb=1";
        opts.numRpcThreads = 0;
        opts.vLogLevel = 2;
        SCClientCommon::SetClusterSetupOptions(opts);
    }
};

TEST_F(RemoteSendRecvDuplicateTest, LEVEL1_TestDuplicateSendOOM)
{
    const int DEFAULT_WAIT_TIME = 60'000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "RemoteWorker.BatchFlushAsyncRead.rpc.timeout", "2048*call()"));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;

    ProducerConf producerConf{
        .delayFlushTime = 20, .pageSize = 512 * KB, .maxStreamSize = 1 * MB, .autoCleanup = false
    };

    std::string streamName("testDupSendOOM");

    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, producerConf));
    SubscriptionConfig consumerConf("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName, consumerConf, consumer));

    const int numEle = 2;
    const int eleSize = 8 * KB;
    std::thread consumerThrd([&consumer]() {
        int received = 0;
        while (received < numEle) {
            std::vector<Element> outElements;
            DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
            if (!outElements.empty()) {
                received += outElements.size();
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            }
        }
    });

    std::thread producerThrd([&producer]() {
        std::vector<uint8_t> writeElement = RandomData().RandomBytes(eleSize);
        Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
        DS_ASSERT_OK(producer->Send(element));
        const int sleepMs = 10'000;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        DS_ASSERT_OK(producer->Send(element));
    });

    producerThrd.join();
    consumerThrd.join();
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "RemoteWorker.BatchFlushAsyncRead.rpc.timeout"));
}
}  // namespace st
}  // namespace datasystem

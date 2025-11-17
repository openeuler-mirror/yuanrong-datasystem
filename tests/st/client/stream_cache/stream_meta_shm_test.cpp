/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test StreamMetaShm.
 */
#include <gtest/gtest.h>
#include <cstdint>
#include <vector>
#include "client/stream_cache/sc_client_common.h"
#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/utils/status.h"
namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
using namespace datasystem::client::stream_cache;
class StreamMetaShmTest : public SCClientCommon {
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

    Status SendHelper(std::shared_ptr<Producer> producer, size_t numElements, bool &stop, size_t idx, int minEleSize,
                      int maxEleSize);
    Status ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements, bool &stopReceive);
    void BasicMPSCTest(int minEleSize, int maxEleSize);

    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;
    HostPort w3Addr_;

    std::vector<std::shared_ptr<StreamClient>> clients;

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const int DEFAULT_WORKER_NUM = 3;
    const int DEFAULT_LOG_LEVEL = 1;
};

void StreamMetaShmTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = DEFAULT_WORKER_NUM;
    opts.enableDistributedMaster = "true";
    opts.numRpcThreads = 0;
    opts.vLogLevel = DEFAULT_LOG_LEVEL;
    SCClientCommon::SetClusterSetupOptions(opts);
}

void StreamMetaShmTest::SetUp()
{
    ExternalClusterTest::SetUp();
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(K_TWO, w3Addr_));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, K_TWO));
    std::shared_ptr<StreamClient> c1, c2, c3;
    InitStreamClient(0, c1);
    InitStreamClient(1, c2);
    InitStreamClient(K_TWO, c3);

    clients.emplace_back(c1);
    clients.emplace_back(c2);
    clients.emplace_back(c3);
}

void StreamMetaShmTest::TearDown()
{
    for (auto &client : clients) {
        client.reset();
    }
    ExternalClusterTest::TearDown();
}

Status StreamMetaShmTest::SendHelper(std::shared_ptr<Producer> producer, size_t numElements, bool &stop, size_t idx,
                                     int minEleSize, int maxEleSize)
{
    const int DEFAULT_SLEEP_TIME = 300;
    int retryLimit = 300;
    uint64_t totalEleSize = 0;
    Raii raii([&totalEleSize, idx]() {
        LOG(INFO) << "TotalSendSize: " << totalEleSize << ", producer: " << idx;
    });
    for (size_t i = 0; i < numElements; i++) {
        RandomData rand;
        int64_t dataSize = rand.GetRandomUint64(minEleSize, maxEleSize);
        std::string writeElement = RandomData().GetRandomString(dataSize);
        Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
        datasystem::Status rc = producer->Send(element);
        if (rc.IsError()) {
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                CHECK_FAIL_RETURN_STATUS(!stop, K_RUNTIME_ERROR, "");
                rc = producer->Send(element);
            }
        }
        if (rc) {
            totalEleSize += dataSize;
        }
        CHECK_FAIL_RETURN_STATUS(!stop, K_RUNTIME_ERROR, "");
        RETURN_IF_NOT_OK(rc);
    }
    return Status::OK();
}

Status StreamMetaShmTest::ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements, bool &stopReceive)
{
    Timer timer;
    size_t remaining = numElements;
    const int PER_RECEIVE_NUM = 1;
    const int DEFAULT_WAIT_TIME = 1000;
    uint64_t receiveSize = 0;
    size_t receiveCount = 0;
    while (remaining > 0 && !stopReceive) {
        std::vector<Element> outElements;
        RETURN_IF_NOT_OK(consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements));
        if (!outElements.empty()) {
            remaining -= outElements.size();
            CHECK_FAIL_RETURN_STATUS(outElements.size() == PER_RECEIVE_NUM, K_RUNTIME_ERROR, "aaa");
            receiveSize += outElements[0].size;
            receiveCount++;
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
        }
    }
    LOG(INFO) << "TotalReceiveSize:" << receiveSize << ", receiveCount: " << receiveCount;
    CHECK_FAIL_RETURN_STATUS(remaining == 0, K_RUNTIME_ERROR, "failed to receive all data");
    return Status::OK();
}

void StreamMetaShmTest::BasicMPSCTest(int minEleSize, int maxEleSize)
{
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(clients[K_TWO]->Subscribe("stream1", config, consumer1));

    ProducerConf conf;
    conf.maxStreamSize = 10 * MB;  // 10MB
    conf.streamMode = StreamMode::MPSC;
    conf.pageSize = 1 * MB;
    conf.retainForNumConsumers = 1;
    conf.autoCleanup = true;

    std::vector<std::shared_ptr<Producer>> producers;
    int producerCount = 3;
    for (int i = 0; i < producerCount; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(clients[i % clients.size()]->CreateProducer("stream1", producer, conf));
        producers.emplace_back(std::move(producer));
    }

    const int threadNum = producers.size() + 1;
    const int totalEleSize = 1 * GB;
    const size_t numElementsPerPub = totalEleSize / producers.size() / ((minEleSize + maxEleSize) / K_TWO);
    const size_t totalEleNum = numElementsPerPub * producers.size();

    ThreadPool pool(threadNum);
    bool stopAll = false;
    std::vector<std::future<Status>> producerFuts;
    for (size_t i = 0; i < producers.size(); i++) {
        auto p = producers[i];
        producerFuts.emplace_back(pool.Submit([this, p, &stopAll, i, numElementsPerPub, minEleSize, maxEleSize]() {
            return SendHelper(p, numElementsPerPub, stopAll, i, minEleSize, maxEleSize);
        }));
    }
    auto consumerFut = pool.Submit(
        [this, consumer1, &stopAll, &totalEleNum]() { return ReceiveHelper(consumer1, totalEleNum, stopAll); });

    Status lastPRc;
    while (!producerFuts.empty()) {
        for (auto itr = producerFuts.begin(); itr != producerFuts.end();) {
            if (!itr->valid()) {
                ++itr;
                continue;
            }
            auto pRc = itr->get();
            if (pRc.IsError() && !stopAll) {
                lastPRc = pRc;
                stopAll = true;
            }
            itr = producerFuts.erase(itr);
        }
        sleep(1);
    }
    auto sRc = consumerFut.get();
    DS_ASSERT_OK(lastPRc);
    DS_ASSERT_OK(sRc);

    for (auto &p : producers) {
        DS_ASSERT_OK(p->Close());
    }
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(TryAndDeleteStream(clients[0], "stream1"));
}

TEST_F(StreamMetaShmTest, TestRemoteConsumerNotReceive)
{
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(clients[1]->Subscribe("stream1", config, consumer1));

    int maxStreamSize = 4 * MB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.streamMode = StreamMode::SPSC;
    conf.pageSize = 1 * MB;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(clients[0]->CreateProducer("stream1", producer1, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());

    auto maxEleCountPerNode =  maxStreamSize / sizeElement;

    for (auto i = 0ul; i < maxEleCountPerNode; ++i) {
        DS_ASSERT_OK(producer1->Send(element1));
    }

    // Wait for all ele to be flushed to w1. After flushing, the shared memory occupied by this stream on w0 should be
    // 0.
    int waitFlushTimeSec = 2;
    sleep(waitFlushTimeSec);

    for (auto i = 0ul; i < maxEleCountPerNode; ++i) {
        DS_ASSERT_OK(producer1->Send(element1));
    }

    auto rc = producer1->Send(element1);
    ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);

    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(TryAndDeleteStream(clients[0], "stream1"));
}

TEST_F(StreamMetaShmTest, TestRemoteConsumerReceiveAfterOOM)
{
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(clients[1]->Subscribe("stream1", config, consumer1));

    int maxStreamSize = 4 * MB;
    int pageSize = 1 * MB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.streamMode = StreamMode::SPSC;
    conf.pageSize = 1 * MB;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(clients[0]->CreateProducer("stream1", producer1, conf));

    const size_t sizeElement = 1 * KB;
    std::string writeElement1 = RandomData().GetRandomString(sizeElement);
    Element element1(reinterpret_cast<uint8_t *>(writeElement1.data()), writeElement1.size());

    auto maxEleCountPerNode =  maxStreamSize / sizeElement;
    auto maxEleCountPerPage =  pageSize / sizeElement;

    for (auto i = 0ul; i < maxEleCountPerNode; ++i) {
        DS_ASSERT_OK(producer1->Send(element1));
    }

    // Wait for all ele to be flushed to w1. After flushing, the shared memory occupied by this stream on w0 should be
    // 0.
    int waitFlushTimeSec = 2;
    sleep(waitFlushTimeSec);

    for (auto i = 0ul; i < maxEleCountPerNode; ++i) {
        DS_ASSERT_OK(producer1->Send(element1));
    }

    auto rc = producer1->Send(element1);
    ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);

    std::vector<Element> eleToReceive;
    DS_ASSERT_OK(consumer1->Receive(maxEleCountPerPage + 1, 0, eleToReceive));
    ASSERT_EQ(eleToReceive.size(), maxEleCountPerPage + 1);
    // Ack a whole page of elements to ensure that the shared memory usage of the stream is reduced.
    DS_ASSERT_OK(consumer1->Ack(eleToReceive.rbegin()->id));

    sleep(waitFlushTimeSec);

    DS_ASSERT_OK(producer1->Send(element1));

    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(TryAndDeleteStream(clients[0], "stream1"));
}

// All are normal elements
TEST_F(StreamMetaShmTest, DISABLED_EXCLUSIVE_LEVEL1_MPSCTest1)
{
    int minEleSize = 200;
    int maxEleSize = 1000;
    BasicMPSCTest(minEleSize, maxEleSize);
}

// Mix of normal elements and big elements
TEST_F(StreamMetaShmTest, DISABLED_EXCLUSIVE_LEVEL1_MPSCTest2)
{
    int minEleSize = 1 * MB;
    int maxEleSize = 3 * MB;
    BasicMPSCTest(minEleSize, maxEleSize);
}

// All are big elements
TEST_F(StreamMetaShmTest, DISABLED_EXCLUSIVE_LEVEL1_MPSCTest3)
{
    int minEleSize = 2 * MB;
    int maxEleSize = 3 * MB;
    BasicMPSCTest(minEleSize, maxEleSize);
}
}  // namespace st
}  // namespace datasystem

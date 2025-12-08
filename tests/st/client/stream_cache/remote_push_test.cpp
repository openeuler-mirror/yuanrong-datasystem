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
 * Description: Remote cache push.
 */
#include <gtest/gtest.h>
#include <string>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"
#include "sc_client_common.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
namespace datasystem {
namespace st {
using namespace datasystem::client::stream_cache;
constexpr int NUM_ELES = 100;
constexpr uint64_t PAGE_SIZE = 4 * 1024;
constexpr uint64_t BIG_SIZE_RATIO = 16;
constexpr uint64_t BIG_SIZE = PAGE_SIZE / BIG_SIZE_RATIO;
constexpr uint64_t TEST_STREAM_SIZE = 64 * 1024 * 1024;
class RemotePushTest : public SCClientCommon {
public:
    static std::string streamName_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;

    void SetUp() override;

    void TearDown() override;

    void StartStream(const std::unique_ptr<BaseCluster> &cluster, const std::string &streamName, const std::string &ak,
                 const std::string &sk, uint64_t numElements);

protected:
    void InitTest();

    // Mock producer worker.
    HostPort pubWorkerAddress_;
    HostPort subWorkerAddress_;

    // Construct consumer worker client.
    std::shared_ptr<Consumer> consumer_;
    std::shared_ptr<StreamClient> consumerClient_ = nullptr;

    static constexpr int CLIENT_RPC_TIMEOUT = 4 * 60 * 1000;
    std::shared_ptr<Producer> producer_;
    std::shared_ptr<StreamClient> producerClient_ = nullptr;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};
std::string RemotePushTest::streamName_ = "stream";

void RemotePushTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numWorkers = 3;
    opts.numEtcd = 1;
    opts.workerGflagParams = "-shared_memory_size_mb=10000";
    opts.numRpcThreads = 0;
    opts.vLogLevel = 2;
    SCClientCommon::SetClusterSetupOptions(opts);
}

void RemotePushTest::SetUp()
{
    ExternalClusterTest::SetUp();
    InitTest();
}

void RemotePushTest::TearDown()
{
    producerClient_ = nullptr;
    consumerClient_ = nullptr;
    ExternalClusterTest::TearDown();
}

void RemotePushTest::InitTest()
{
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, pubWorkerAddress_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, subWorkerAddress_));

    ProducerConf conf;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    InitStreamClient(0, producerClient_);
    ASSERT_EQ(producerClient_->CreateProducer(streamName_, producer_, conf), Status::OK());

    auto api = std::make_unique<ClientWorkerApi>(pubWorkerAddress_, RpcCredential(), signature_.get());
    ASSERT_EQ(api->Init(CLIENT_RPC_TIMEOUT, CLIENT_RPC_TIMEOUT), Status::OK());

    InitStreamClient(1, consumerClient_);
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    ASSERT_EQ(consumerClient_->Subscribe(streamName_, config, consumer_), Status::OK());
}

TEST_F(RemotePushTest, TestSingleProducer)
{
    ElementGenerator elementGenerator(BIG_SIZE * 5 / 4);
    std::string producerName = "producer1";
    auto strs = elementGenerator.GenElements(producerName, NUM_ELES);

    // Send.
    for (int i = 0; i < NUM_ELES; i++) {
        ASSERT_EQ(producer_->Send(Element((uint8_t *)strs[i].data(), strs[i].size())), Status::OK());
        LOG(INFO) << FormatString("Sz: [%d]: [%zu]", i, strs[i].size());
    }

    // Recv.
    std::vector<Element> outElements;
    ASSERT_EQ(consumer_->Receive(NUM_ELES, 0, outElements), Status::OK());
    std::unordered_map<std::string, uint64_t> seqNoMap;
    for (const auto &element : outElements) {
        LOG(INFO) << FormatString("Cursor: [%zu], Sz: [%zu]", element.id, element.size);
        ElementView view(std::string((const char *)element.ptr, element.size));
        ASSERT_EQ(view.VerifyIntegrity(), Status::OK());
        ASSERT_EQ(view.VerifyFifo(seqNoMap), Status::OK());
    }
}

void RemotePushTest::StartStream(const std::unique_ptr<BaseCluster> &cluster, const std::string &streamName,
                                 const std::string &ak, const std::string &sk, uint64_t numElements)
{
    const int numPubs = 24;
    const int numSubs = 3;
    const int numNodes = 3;
    auto producerSend = [this, &cluster, ak, sk, &streamName](int index, uint64_t numElements,
                                                        const std::string &producerName) {
        std::shared_ptr<StreamClient> client;
        InitStreamClient(index, client);

        ProducerConf conf;
        conf.maxStreamSize = 100 * 1024 * 1024;
        conf.pageSize = 2 * 1024 * 1024;
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));
        ElementGenerator elementGenerator(1024 * 1024, 100);
        while (numElements > 0) {
            auto str = elementGenerator.GenElement(producerName);
            Status rc = producer->Send(Element((uint8_t *)str.data(), str.size()));
            if (rc.GetCode() == K_OUT_OF_MEMORY) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            DS_ASSERT_OK(rc);
            --numElements;
        }
        producer->Close();
        client.reset();
    };

    auto consumerRecv = [&cluster, ak, sk, &streamName, this](int index, uint64_t numElements) {
        std::shared_ptr<StreamClient> client;
        InitStreamClient(index, client);
        std::shared_ptr<Consumer> consumer;
        std::string subName = "sub" + std::to_string(index);
        SubscriptionConfig sub(subName, SubscriptionType::STREAM);
        DS_ASSERT_OK(client->Subscribe(streamName, sub, consumer));
        uint64_t numElementsToReceive = numPubs * numElements;
        LOG(INFO) << FormatString("[%s] Consumer expects to receive %zu elements", subName, numElementsToReceive);
        const uint64_t timeoutMs = 60000;
        while (numElementsToReceive > 0) {
            std::vector<Element> outElements;
            ASSERT_EQ(consumer->Receive(timeoutMs, outElements), Status::OK());
            for (const auto &element : outElements) {
                LOG(INFO) << FormatString("[%s] Cursor: [%zu], Sz: [%zu]", subName, element.id, element.size);
                ElementView view(std::string((const char *)element.ptr, element.size));
                ASSERT_EQ(view.VerifyIntegrity(), Status::OK());
                --numElementsToReceive;
                consumer->Ack(element.id);
            }
        }
        consumer->Close();
        client.reset();
    };

    std::vector<std::thread> threads;

    for (int i = 0; i < numPubs; i++) {
        threads.emplace_back([i, &producerSend, numElements] {
            std::string pubName = "pub" + std::to_string(i);
            producerSend(i % numNodes, numElements, pubName);
        });
    }

    for (int i = 0; i < numSubs; ++i) {
        threads.emplace_back([i, &consumerRecv, numElements] { consumerRecv(i % numNodes, numElements); });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(RemotePushTest, DISABLED_LEVEL1_TestDifferentWorkersMultipleProducers)
{
    FLAGS_v = 0;
    std::vector<std::thread> threads;

    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "RemoteWorkerStreamBlockingWakeupTimeout", "1*call(45000)");
    const int numElementsPerPub = 128;
    const int numStreams = 1;
    for (int i = 0; i < numStreams; i++) {
        threads.emplace_back([i, this] {
            std::string streamName = "stream000" + std::to_string(i);
            StartStream(cluster_, streamName, accessKey_, secretKey_, numElementsPerPub);
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

class RemotePushOOMTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerNumber = 2;
        opts.numWorkers = workerNumber;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=10000";
        opts.enableDistributedMaster = "true";
        opts.numRpcThreads = 0;
        opts.vLogLevel = 0;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        producerClient_ = nullptr;
        consumerClient_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTest()
    {
        InitStreamClient(0, producerClient_);
        InitStreamClient(1, consumerClient_);
    }

    // Mock producer worker.
    HostPort pubWorkerAddress_;
    HostPort subWorkerAddress_;

    // Construct consumer worker client.
    std::shared_ptr<StreamClient> consumerClient_ = nullptr;

    static constexpr int CLIENT_RPC_TIMEOUT = 4 * 60 * 1000;
    std::shared_ptr<StreamClient> producerClient_ = nullptr;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    // std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
    size_t maxPageCount_ = 2;
    size_t pageSize_ = 1024 * 1024;
};

TEST_F(RemotePushOOMTest, TestRecvOOM)
{
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::shared_ptr<Producer>> producers;
    int streamCount = 1;
    int oomTimeout = 3;

    ProducerConf conf;
    conf.pageSize = pageSize_;
    conf.maxStreamSize = pageSize_ * maxPageCount_;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    for (int index = 0; index < streamCount; index++) {
        std::string streamName = "TestOOMStream-" + std::to_string(index);
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        ASSERT_EQ(producerClient_->CreateProducer(streamName, producer, conf), Status::OK());
        ASSERT_EQ(consumerClient_->Subscribe(streamName, config, consumer, true), Status::OK());
        consumers.emplace_back(std::move(consumer));
        producers.emplace_back(std::move(producer));
    }

    const size_t elementSize = 10240;  // 10k.
    size_t nums = pageSize_ * maxPageCount_ * 2 / elementSize - 10;

    std::string data(elementSize, 'a');
    Element element((uint8_t *)data.data(), data.size());
    // Send.
    for (int index = 0; index < streamCount; index++) {
        for (size_t i = 0; i < nums; i++) {
            Status rc = producers[index]->Send(element);
            Timer timer;
            const int maxTimeout = 10;
            while (rc.GetCode() == K_OUT_OF_MEMORY && timer.ElapsedSecond() < maxTimeout) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                rc = producers[index]->Send(element);
            }
            DS_ASSERT_OK(rc);
            LOG(INFO) << FormatString("Stream index %zu,: send count: %zu", streamCount, i);
        }
    }

    sleep(oomTimeout);

    for (int index = 0; index < streamCount; index++) {
        size_t recvNum = 0;
        while (recvNum < nums) {
            std::vector<Element> outElements;
            const int recvTimeout = 1000;
            ASSERT_EQ(consumers[index]->Receive(nums, recvTimeout, outElements), Status::OK());
            recvNum += outElements.size();
            LOG(INFO) << "Recv num:" << recvNum;
        }
        ASSERT_EQ(recvNum, nums);
    }
}

}  // namespace st
}  // namespace datasystem

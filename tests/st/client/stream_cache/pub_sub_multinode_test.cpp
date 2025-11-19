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
class PubSubMultiNodeTest : public SCClientCommon {
#else
class PubSubMultiNode : public CommonTest {
#endif
public:
#ifdef MULTI_NODE
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override;
#endif
    void SetUp() override;

    void TearDown() override;

    static std::once_flag onceFlag_;

protected:
    // Mock producer worker.
    HostPort w1Addr_;
    HostPort w2Addr_;
    HostPort w3Addr_;

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

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    std::shared_ptr<StreamClient> w3Client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const uint32_t WORKER_COUNT = 3;
};
std::once_flag PubSubMultiNodeTest::onceFlag_;

#ifdef MULTI_NODE
void PubSubMultiNodeTest::SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    opts.numEtcd = 1;
    opts.numWorkers = WORKER_COUNT;
    opts.workerGflagParams = " -page_size=" + std::to_string(PAGE_SIZE);
    opts.numRpcThreads = 16;
    SCClientCommon::SetClusterSetupOptions(opts);
}
#endif

void PubSubMultiNodeTest::SetUp()
{
#ifdef MULTI_NODE
    ExternalClusterTest::SetUp();
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w1Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, w2Addr_));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, w3Addr_));
#else
    w1Addr_ = HostPort("127.0.0.1", 2295);
    w2Addr_ = HostPort("127.0.0.1", 11589);
    w3Addr_ = HostPort("127.0.0.1", 8666);
#endif
    InitStreamClient(0, w1Client_);
    InitStreamClient(1, w2Client_);
    InitStreamClient(2, w3Client_); // index is 2
    defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
}

void PubSubMultiNodeTest::TearDown()
{
    w1Client_ = nullptr;
    w2Client_ = nullptr;
    w3Client_ = nullptr;
#ifdef MULTI_NODE
    ExternalClusterTest::TearDown();
#endif
}

TEST_F(PubSubMultiNodeTest, StreamModeTwoConsumer)
{
    std::string streamName = "testStreamMode2Con";
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer;
    ASSERT_EQ(w2Client_->Subscribe(streamName, config, consumer), Status::OK());

    std::shared_ptr<Consumer> consumer2;
    w1Client_->Subscribe(streamName, config, consumer2);
    ASSERT_TRUE(w1Client_->Subscribe(streamName, config, consumer2) != Status::OK());
}

TEST_F(PubSubMultiNodeTest, PubCloseFirst)
{
    std::string streamName = "testPubCloseFirst";
    ThreadPool pool(5);
    std::promise<void> promise;
    std::future<void> subFut = promise.get_future();
    std::vector<std::future<Status>> futs;
    futs.emplace_back(pool.Submit([this, &subFut, streamName]() {
        std::shared_ptr<Producer> producer;
        RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
        subFut.get();
        return producer->Close();
    }));
    futs.emplace_back(pool.Submit([this, &subFut, streamName]() {
        std::shared_ptr<Consumer> consumer2;
        SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config2, consumer2));
        Status status;
        sleep(1); // wait for pub to close first
        status = consumer2->Close();
        return status;
    }));
    futs.emplace_back(pool.Submit([this, &promise, streamName]() {
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        std::shared_ptr<Consumer> consumer;
        RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
        promise.set_value();
        sleep(1); // wait for pub to close first
        Status status;
        status = consumer->Close();
        LOG(INFO) << FormatString("%s", status.ToString());
        return status;
    }));
    for (auto &fut : futs) {
        ASSERT_EQ(fut.get(), Status::OK());
    }
    ASSERT_EQ(w1Client_->DeleteStream(streamName), Status::OK());
}

TEST_F(PubSubMultiNodeTest, AvoidMissAddRemote)
{
    std::string strmName = "testAvoidMissAddRemote";
    ThreadPool pool(10);
    for (int i = 0; i < 10; i++) {
        pool.Submit([this, i, strmName]() {
            ThreadPool pool(5);
            std::vector<std::promise<void>> promises(2);
            std::vector<std::shared_future<void>> subFuts;
            for (auto &promise : promises) {
                subFuts.emplace_back(promise.get_future());
            }
            std::vector<std::future<Status>> futs;
            std::string streamName = strmName + std::to_string(i);
            std::shared_ptr<Producer> producer;

            futs.emplace_back(pool.Submit([this, &subFuts, &producer, streamName]() {
                RETURN_IF_NOT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
                for (auto subFut : subFuts) {
                    subFut.get();
                }
                std::string str(10, 'c');
                Element e;
                e.ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(str.data()));
                e.size = str.size();
                RETURN_IF_NOT_OK(producer->Send(e));
                return Status::OK();
            }));
            futs.emplace_back(pool.Submit([this, streamName, &promises]() {
                std::shared_ptr<Consumer> consumer2;
                SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
                RETURN_IF_NOT_OK(w1Client_->Subscribe(streamName, config2, consumer2));
                promises[0].set_value();
                std::vector<Element> elements;
                RETURN_IF_NOT_OK(consumer2->Receive(1, 1'000, elements));
                return consumer2->Close();
            }));
            futs.emplace_back(pool.Submit([this, streamName, &promises]() {
                SubscriptionConfig config("sub1", SubscriptionType::STREAM);
                std::shared_ptr<Consumer> consumer;
                RETURN_IF_NOT_OK(w2Client_->Subscribe(streamName, config, consumer));
                promises[1].set_value();
                std::vector<Element> elements;
                RETURN_IF_NOT_OK(consumer->Receive(1, 1'000, elements));
                return consumer->Close();
            }));
            for (auto &fut : futs) {
                ASSERT_EQ(fut.get(), Status::OK());
            }
            ASSERT_EQ(producer->Close(), Status::OK());
            ASSERT_EQ(TryAndDeleteStream(w1Client_, streamName), Status::OK());
        });
    }
}

TEST_F(PubSubMultiNodeTest, TestCreateOrderSingleNode)
{
    const std::string stream1 = "testCreateOrder_sameNode_s1";
    // Create consumer first, then producer
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w1Client_->Subscribe(stream1, config1, con1));
    std::shared_ptr<Producer> prod1;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));

    // Switch the order. Producer first, then consumer
    const std::string stream2 = "testCreateOrder_sameNode_s2";
    std::shared_ptr<Producer> prod2;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream2, prod2, defaultProducerConf_));
    std::shared_ptr<Consumer> con2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config2, con2));

    // Sanity test by sending/receiving one elemnt
    RandomData rand;
    auto str = rand.GetRandomString(defaultProducerConf_.pageSize);
    DS_ASSERT_OK(prod1->Send(Element((uint8_t *)str.data(), str.size())));
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con1->Ack(outElements[0].id);
    std::string res1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res1, str);

    DS_ASSERT_OK(prod2->Send(Element((uint8_t *)str.data(), str.size())));
    outElements.clear();
    DS_ASSERT_OK(con2->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con2->Ack(outElements[0].id);
    std::string res2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res2, str);

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(prod2->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(con2->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    DS_ASSERT_OK(w1Client_->DeleteStream(stream2));
}

TEST_F(PubSubMultiNodeTest, TestCreateOrderCrossNode)
{
    const std::string stream1 = "testCreateOrder_diffNode_s1";
    // Create consumer first, then producer
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con1));
    std::shared_ptr<Producer> prod1;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));

    // Switch the order. Producer first, then consumer
    const std::string stream2 = "testCreateOrder_diffNode_s2";
    std::shared_ptr<Producer> prod2;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream2, prod2, defaultProducerConf_));
    std::shared_ptr<Consumer> con2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream2, config2, con2));

    // Sanity test by sending/receiving one elemnt
    RandomData rand;
    auto str = rand.GetRandomString(defaultProducerConf_.pageSize);
    DS_ASSERT_OK(prod1->Send(Element((uint8_t *)str.data(), str.size())));
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con1->Ack(outElements[0].id);
    std::string res1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res1, str);

    DS_ASSERT_OK(prod2->Send(Element((uint8_t *)str.data(), str.size())));
    outElements.clear();
    DS_ASSERT_OK(con2->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con2->Ack(outElements[0].id);
    std::string res2(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res2, str);

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(prod2->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(con2->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    DS_ASSERT_OK(w1Client_->DeleteStream(stream2));
}

TEST_F(PubSubMultiNodeTest, TestCreateOrderSingleNodeOOM)
{
    RandomData rand;
    auto str = rand.GetRandomString(defaultProducerConf_.pageSize);

    Status rc;
    const std::string stream1 = "testCreateOrder_sameNodeOOM";
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "CreatePageZero.AllocMemory",
                                           "1*return(K_OUT_OF_MEMORY)"));
    // Create consumer first, then producer
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w1Client_->Subscribe(stream1, config1, con1));
    std::shared_ptr<Producer> prod1;
    rc = w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_);
    // This should fail with OOM the first time.
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
    // Try again should be successful
    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 0, "CreatePageZero.AllocMemory"));
    prod1.reset();
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));
    DS_ASSERT_OK(prod1->Send(Element((uint8_t *)str.data(), str.size())));
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con1->Ack(outElements[0].id);
    std::string res1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res1, str);
    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(con1->Close());
}

TEST_F(PubSubMultiNodeTest, TestCreateOrderCrossNodeOOM)
{
    RandomData rand;
    auto str = rand.GetRandomString(defaultProducerConf_.pageSize);
    Status rc;
    const std::string stream1 = "testCreateOrder_diffNodeOOM_s1";
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "CreatePageZero.AllocMemory",
                                           "1*return(K_OUT_OF_MEMORY)"));
    // Create consumer first, then producer
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    // We don't know the page size at this point. So we can't reserve the memory.
    // So subscribe should be successful.
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con1));
    const int MULTIPLIER = 16;
    defaultProducerConf_.maxStreamSize = MULTIPLIER * defaultProducerConf_.pageSize;
    defaultProducerConf_.reserveSize = defaultProducerConf_.maxStreamSize;
    std::shared_ptr<Producer> prod1;
    rc = w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_);
    // This should fail with OOM when the remote worker got the topo change, and return OOM to the producer
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, "CreatePageZero.AllocMemory"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "ReserveAdditionalMemory.AllocMemory",
                                           "2*return(K_OK)->return(K_OUT_OF_MEMORY)"));
    // This should fail with OOM when the remote worker got the topo change, and return OOM to the producer
    prod1.reset();
    rc = w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_);
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
    // Try again should be successful
    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, "ReserveAdditionalMemory.AllocMemory"));
    prod1.reset();
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));
    DS_ASSERT_OK(prod1->Send(Element((uint8_t *)str.data(), str.size())));
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    con1->Ack(outElements[0].id);
    std::string res1(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    DS_ASSERT_TRUE(res1, str);
    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(con1->Close());

    // Switch the order. Producer first, then consumer
    const std::string stream2 = "testCreateOrder_diffNodeOOM_s2";
    std::shared_ptr<Producer> prod2;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream2, prod2, defaultProducerConf_));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "CreatePageZero.AllocMemory",
                                           "1*return(K_OUT_OF_MEMORY)"));
    std::shared_ptr<Consumer> con2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    // Subscribe should pick up the page size and returns OOM.
    rc = w2Client_->Subscribe(stream2, config2, con2);
    // This should fail with OOM
    ASSERT_EQ(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
}

TEST_F(PubSubMultiNodeTest, TestMultiStreamsOOM)
{
    Status rc;
    int i = 0;
    std::unordered_map<std::string, std::shared_ptr<Producer>> prodList;
    while (rc.IsOk()) {
        std::string streamName = FormatString("testMultiStreamsOOM%d", i);
        std::shared_ptr<Producer> prod;
        // Each producer will reserve one page of memory.
        rc = w1Client_->CreateProducer(streamName, prod, defaultProducerConf_);
        if (rc.IsOk()) {
            LOG(INFO) << FormatString("[%s] Create producer success", streamName);
            prodList.emplace(streamName, std::move(prod));
            ++i;
            continue;
        }
        // Expect we will run out of resources at some point
        DS_ASSERT_TRUE(rc.GetCode(), K_SC_STREAM_RESOURCE_ERROR);
    }

    // Expect prodList is not empty.
    ASSERT_TRUE(!prodList.empty());
    // Page size is 1m and shared_memory_size_mb is 64m.
    // Maximum number of streams we can create is 64 but there can be some overhead and
    // in reality we create less than the maximum.
    const int maxProducer = 64;
    ASSERT_TRUE(prodList.size() <= maxProducer);

    // Delete one of the stream. We expect the stream memory is released and we can create one more.
    auto iter = prodList.begin();
    auto oneProducer = std::move(iter->second);
    auto oneStreamName = iter->first;
    prodList.erase(oneStreamName);
    DS_ASSERT_OK(oneProducer->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(oneStreamName));
    LOG(INFO) << FormatString("[%s] delete success", oneStreamName);

    std::string streamName = FormatString("stream%d", i++);
    std::shared_ptr<Producer> prod;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, prod, defaultProducerConf_));
    LOG(INFO) << FormatString("[%s] Create producer success", streamName);
    prodList.emplace(streamName, std::move(prod));

    for (auto &ele: prodList) {
        DS_ASSERT_OK(ele.second->Close());
        DS_ASSERT_OK(w1Client_->DeleteStream(ele.first));
    }
}

TEST_F(PubSubMultiNodeTest, TestMultiProducersUndo)
{
    defaultProducerConf_.retainForNumConsumers = 1;
    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i,
                                               "master.PubIncreaseNodeImpl.beforeSendNotification",
                                               "1*return(K_RUNTIME_ERROR)"));
    }
    const std::string stream1 = "testMultiProdUndo";
    const int NUM_THREADS = 2;
    const int NUM_ELEMENTS = 100;
    const int ELEMENT_SIZE = 48;
    ThreadPool pool(NUM_THREADS);
    std::vector<std::future<Status>> futs;
    for (int i = 0; i < NUM_THREADS; ++i) {
        futs.emplace_back(pool.Submit([this, &stream1]() {
            std::shared_ptr<Producer> prod;
            Status rc = w1Client_->CreateProducer(stream1, prod, defaultProducerConf_);
            if (rc.IsError()) { return rc; }
            RandomData rand;
            auto str = rand.GetRandomString(ELEMENT_SIZE);
            for (int k = 0; k < NUM_ELEMENTS; ++k) {
                rc = prod->Send(Element((uint8_t *)str.data(), str.size()));
                if (rc.IsError()) { return rc; }
            }
            return Status::OK();
        }));
    }

    std::shared_ptr<Consumer> con;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config, con));

    std::vector<Status> status;
    for (int i = 0; i < NUM_THREADS; ++i) {
        status.push_back(futs[i].get());
    }
    // Expect exactly one of the producer should hit K_RUNTIME_ERROR;
    if (status[0].IsOk()) {
        ASSERT_TRUE(status[1].GetCode() == K_RUNTIME_ERROR);
    } else {
        ASSERT_TRUE(status[0].GetCode() == K_RUNTIME_ERROR);
        ASSERT_TRUE(status[1].IsOk());
    }

    std::vector<Element> outElements;
    DS_ASSERT_OK(con->Receive(NUM_ELEMENTS, RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), NUM_ELEMENTS);
    DS_ASSERT_OK(con->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    sleep(1);
}

TEST_F(PubSubMultiNodeTest, BigElement2S2P2C)
{
    // 2 streams: 2 producers -> 2 consumers for each stream.
    const std::string stream1 = "testBigEle2S2P2C_s1";
    const std::string stream2 = "testBigEle2S2P2C_s2";
    defaultProducerConf_.pageSize = 4 * KB;
    const int timeout = 10000;

    // Create the 4 producers.
    std::shared_ptr<Producer> prod11; // client1, stream 1, producer 1
    std::shared_ptr<Producer> prod12; // client2, stream 1, producer 2
    std::shared_ptr<Producer> prod21; // client3, stream 2, producer 1
    std::shared_ptr<Producer> prod22; // client2, stream 2, producer 2
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod11, defaultProducerConf_));
    DS_ASSERT_OK(w2Client_->CreateProducer(stream1, prod12, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod21, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod22, defaultProducerConf_));

    // Create the 4 consumers.
    std::shared_ptr<Consumer> con11; // client 2, stream 1, consumer 1
    std::shared_ptr<Consumer> con12; // client 3, stream 1, consumer 2
    std::shared_ptr<Consumer> con21; // client 1, stream 2, consumer 1
    std::shared_ptr<Consumer> con22; // client 1, stream 2, consumer 2
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con11));
    DS_ASSERT_OK(w3Client_->Subscribe(stream1, config2, con12));
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config1, con21));
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config2, con22));

    // Create big element.
    const uint64_t numOfElementPerProducer = 50;
    const uint64_t expectNumOfElementReceivePerConsumer = numOfElementPerProducer * 2;
    const uint64_t elementSize = 8 * KB;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    // Each producer send 50 big elements.
    for (uint64_t i = 1; i <= numOfElementPerProducer; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        ASSERT_EQ(prod11->Send(element), Status::OK());
        ASSERT_EQ(prod12->Send(element), Status::OK());
        ASSERT_EQ(prod21->Send(element), Status::OK());
        ASSERT_EQ(prod22->Send(element), Status::OK());
    }

    // Each consumer receive 100 big elements.
    std::vector<Element> outElements;
    DS_ASSERT_OK(con11->Receive(expectNumOfElementReceivePerConsumer, timeout, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceivePerConsumer);
    outElements.clear();
    DS_ASSERT_OK(con12->Receive(expectNumOfElementReceivePerConsumer, timeout, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceivePerConsumer);
    outElements.clear();
    DS_ASSERT_OK(con21->Receive(expectNumOfElementReceivePerConsumer, timeout, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceivePerConsumer);
    outElements.clear();
    DS_ASSERT_OK(con22->Receive(expectNumOfElementReceivePerConsumer, timeout, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceivePerConsumer);
    outElements.clear();
    
    DS_ASSERT_OK(prod11->Close());
    DS_ASSERT_OK(prod12->Close());
    DS_ASSERT_OK(prod21->Close());
    DS_ASSERT_OK(prod22->Close());
    DS_ASSERT_OK(con11->Close());
    DS_ASSERT_OK(con12->Close());
    DS_ASSERT_OK(con21->Close());
    DS_ASSERT_OK(con22->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    DS_ASSERT_OK(w1Client_->DeleteStream(stream2));
}

class PubSubMultiNodeDataVerificationTest : public PubSubMultiNodeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        PubSubMultiNodeTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -v=2 -enable_stream_data_verification=true ";
    }
};

TEST_F(PubSubMultiNodeDataVerificationTest, 2S2P2C)
{
    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.PubIncreaseNode.initProducerno",
                                               FormatString("1*call(%zu)", INT64_MAX)));
    }
    // 2 streams: 2 producers -> 2 consumers for each stream.
    const std::string stream1 = "test2S2P2C_s1";
    const std::string stream2 = "test2S2P2C_s2";

    // Create the 4 producers.
    std::shared_ptr<Producer> prod11; // client1, stream 1, producer 1
    std::shared_ptr<Producer> prod12; // client2, stream 1, producer 2
    std::shared_ptr<Producer> prod21; // client3, stream 2, producer 1
    std::shared_ptr<Producer> prod22; // client2, stream 2, producer 2
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod11, defaultProducerConf_));
    DS_ASSERT_OK(w2Client_->CreateProducer(stream1, prod12, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod21, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod22, defaultProducerConf_));

    // Create the 4 consumers.
    std::shared_ptr<Consumer> con11; // client 2, stream 1, consumer 1
    std::shared_ptr<Consumer> con12; // client 3, stream 1, consumer 2
    std::shared_ptr<Consumer> con21; // client 1, stream 2, consumer 1
    std::shared_ptr<Consumer> con22; // client 1, stream 2, consumer 2
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con11));
    DS_ASSERT_OK(w3Client_->Subscribe(stream1, config2, con12));
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config1, con21));
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config2, con22));

    // Each producer send 51 elements, the 51th element is out of order.
    const uint64_t outOfOrderElementNo = 51;
    const uint64_t expectNumOfElementReceive = (outOfOrderElementNo - 1) * 2;
    const uint64_t elementSize = defaultProducerConf_.pageSize / 10;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    // Each producer send the first 50 elements.
    for (uint64_t i = 1; i < outOfOrderElementNo; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        ASSERT_EQ(prod11->Send(element), Status::OK());
        ASSERT_EQ(prod12->Send(element), Status::OK());
        ASSERT_EQ(prod21->Send(element), Status::OK());
        ASSERT_EQ(prod22->Send(element), Status::OK());
    }

    // The first 100 elements received should be in order.
    std::vector<Element> outElements;
    DS_ASSERT_OK(con11->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    for (auto &ele : outElements) {
        std::string actualData(reinterpret_cast<const char *>(ele.ptr), ele.size);
        EXPECT_EQ(str, actualData);
    }
    outElements.clear();
    DS_ASSERT_OK(con12->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    outElements.clear();
    DS_ASSERT_OK(con21->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    outElements.clear();
    DS_ASSERT_OK(con22->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    outElements.clear();

    // Send the out of order element.
    datasystem::inject::Set("DataVerificationOutOfOrder", "call(1)");
    Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
    ASSERT_EQ(prod11->Send(element), Status::OK());
    ASSERT_EQ(prod12->Send(element), Status::OK());
    ASSERT_EQ(prod21->Send(element), Status::OK());
    ASSERT_EQ(prod22->Send(element), Status::OK());
    datasystem::inject::Clear("DataVerificationOutOfOrder");

    // Receiving the elements with incorrect sequence number.
    ASSERT_EQ(con11->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();
    ASSERT_EQ(con12->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();
    ASSERT_EQ(con21->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();
    ASSERT_EQ(con22->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();

    DS_ASSERT_OK(prod11->Close());
    DS_ASSERT_OK(prod12->Close());
    DS_ASSERT_OK(prod21->Close());
    DS_ASSERT_OK(prod22->Close());
    DS_ASSERT_OK(con11->Close());
    DS_ASSERT_OK(con12->Close());
    DS_ASSERT_OK(con21->Close());
    DS_ASSERT_OK(con22->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    DS_ASSERT_OK(w1Client_->DeleteStream(stream2));
}

TEST_F(PubSubMultiNodeDataVerificationTest, BigElement2S2P1C)
{
    // 2 streams: 2 producers -> 2 consumers for each stream.
    const std::string stream1 = "testBigEle2S2P1C_s1";
    const std::string stream2 = "testBigEle2S2P1C_s2";
    defaultProducerConf_.pageSize = 4 * KB;

    // Create the 4 producers.
    std::shared_ptr<Producer> prod11; // client1, stream 1, producer 1
    std::shared_ptr<Producer> prod12; // client2, stream 1, producer 2
    std::shared_ptr<Producer> prod21; // client3, stream 2, producer 1
    std::shared_ptr<Producer> prod22; // client2, stream 2, producer 2
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod11, defaultProducerConf_));
    DS_ASSERT_OK(w2Client_->CreateProducer(stream1, prod12, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod21, defaultProducerConf_));
    DS_ASSERT_OK(w3Client_->CreateProducer(stream2, prod22, defaultProducerConf_));

    // Create the 4 consumers.
    std::shared_ptr<Consumer> con1; // client 2, stream 1, consumer 1
    std::shared_ptr<Consumer> con2; // client 1, stream 2, consumer 1
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w3Client_->Subscribe(stream1, config1, con1));
    DS_ASSERT_OK(w1Client_->Subscribe(stream2, config1, con2));

    // Each producer send 51 elements, the 51th element is out of order.
    const uint64_t outOfOrderElementNo = 50;
    const uint64_t expectNumOfElementReceive = (outOfOrderElementNo - 1) * 2;
    const uint64_t elementSize = 8 * KB;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    // Each producer send the first 50 elements.
    for (uint64_t i = 1; i < outOfOrderElementNo; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        ASSERT_EQ(prod11->Send(element), Status::OK());
        ASSERT_EQ(prod12->Send(element), Status::OK());
        ASSERT_EQ(prod21->Send(element), Status::OK());
        ASSERT_EQ(prod22->Send(element), Status::OK());
    }

    // The first 100 elements received should be in order.
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    for (auto &ele : outElements) {
        std::string actualData(reinterpret_cast<const char *>(ele.ptr), ele.size);
        EXPECT_EQ(str, actualData);
    }
    outElements.clear();
    DS_ASSERT_OK(con2->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    outElements.clear();

    // Send the out of order element.
    datasystem::inject::Set("DataVerificationOutOfOrder", "call(1)");
    Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
    ASSERT_EQ(prod11->Send(element), Status::OK());
    ASSERT_EQ(prod12->Send(element), Status::OK());
    ASSERT_EQ(prod21->Send(element), Status::OK());
    ASSERT_EQ(prod22->Send(element), Status::OK());
    datasystem::inject::Clear("DataVerificationOutOfOrder");

    // Receiving the elements with incorrect sequence number.
    ASSERT_EQ(con1->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();
    ASSERT_EQ(con2->Receive(1, RPC_TIMEOUT, outElements).GetCode(), K_DATA_INCONSISTENCY);
    outElements.clear();
    outElements.clear();
    DS_ASSERT_OK(prod11->Close());
    DS_ASSERT_OK(prod12->Close());
    DS_ASSERT_OK(prod21->Close());
    DS_ASSERT_OK(prod22->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(con2->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
    DS_ASSERT_OK(w1Client_->DeleteStream(stream2));
}

TEST_F(PubSubMultiNodeDataVerificationTest, LEVEL2_ConsumerSubscribleLater)
{
    // 1 stream: 1 producer -> 2 consumers.
    const std::string stream1 = "testConSubscribLater";
    size_t size = 1 * MB;
    RandomData rand;
    std::string data = rand.GetRandomString(size);
    defaultProducerConf_.maxStreamSize = 20 * MB;
    defaultProducerConf_.pageSize = 4 * MB;
    const int timeout = 10000;

    // Create 1 producer.
    std::shared_ptr<Producer> prod1;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));

    // Create first consumer.
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con1, true));

    // The producer send Elements to first consumer.
    int send_count = 0;
    Status status;
    while (true) {
        Element element1(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        status = prod1->Send(element1);
        if (status.IsOk()) {
            send_count += 1;
        } else {
            break;
        }
    }

    // First consumer receive elements.
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(send_count, timeout, outElements));
    for (auto &ele : outElements) {
        DS_ASSERT_OK(con1->Ack(ele.id));
    }
    outElements.clear();

    // The producer send more elements (existing page on consumer side dropped, element with seqNo = 0 is gone).
    send_count = 0;
    while (true) {
        Element element1(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        status = prod1->Send(element1);
        if (status.IsOk()) {
            send_count += 1;
        } else {
            break;
        }
    }

    // Create second consumer.
    std::shared_ptr<Consumer> con2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config2, con2));

    // Second consumer receive elements starting with seqNo != 0.
    DS_ASSERT_OK(con2->Receive(send_count, timeout, outElements));
    outElements.clear();

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(con2->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
}

TEST_F(PubSubMultiNodeDataVerificationTest, ProducerInsertFailed)
{
    // 1 stream: 1 prodcuer -> 1 consumer
    const std::string stream1 = "testProdInsertFailed";

    // Create 1 producer.
    std::shared_ptr<Producer> prod1;
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));

    // Create 1 consumer.
    std::shared_ptr<Consumer> con1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(stream1, config1, con1));

    // The producer send 100 elements, failed to insert element 1 out of 4 times.
    const uint64_t numOfElement = 100;
    const uint64_t failToInsertFrequency = 4;
    const uint64_t expectNumOfElementReceive = numOfElement - (numOfElement / failToInsertFrequency);
    const uint64_t elementSize = defaultProducerConf_.pageSize / 10;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    for (uint64_t i = 1; i <= numOfElement; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        if (i % failToInsertFrequency == 0) {
            datasystem::inject::Set("producer_insert", "1*return(K_INVALID)");
            DS_ASSERT_NOT_OK(prod1->Send(element));
            datasystem::inject::Clear("producer_insert");
        } else {
            ASSERT_EQ(prod1->Send(element), Status::OK());
        }
    }

    // The consumer receive 75 elements.
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    outElements.clear();

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
}

class PubSubMultiNode1Of2ProducersEnableDataVerificationTest : public PubSubMultiNodeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        PubSubMultiNodeTest::SetClusterSetupOptions(opts);
        opts.workerSpecifyGflagParams[0] += " -enable_stream_data_verification=true ";
        opts.workerSpecifyGflagParams[1] += " -enable_stream_data_verification=false ";
    }
};

TEST_F(PubSubMultiNode1Of2ProducersEnableDataVerificationTest, EnableDataVerification1Of2ProducersDifferentNode)
{
    // 1 streams: 2 producers -> 1 consumers.
    const std::string stream1 = "testDataVerify1Of2ProdDiffNode";

    // Create the 2 producers.
    std::shared_ptr<Producer> prod1; // client1, stream 1, producer 1
    std::shared_ptr<Producer> prod2; // client2, stream 1, producer 2
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));
    DS_ASSERT_OK(w2Client_->CreateProducer(stream1, prod2, defaultProducerConf_));

    // Create the 1 consumers.
    std::shared_ptr<Consumer> con1; // client 3, stream 1, consumer 1
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w3Client_->Subscribe(stream1, config1, con1));

    // Each producer send 100 elements.
    const uint64_t numOfElementPerProducer = 100;
    const uint64_t expectNumOfElementReceive = numOfElementPerProducer * 2;
    const uint64_t elementSize = defaultProducerConf_.pageSize / 10;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    for (uint64_t i = 1; i <= numOfElementPerProducer; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        ASSERT_EQ(prod1->Send(element), Status::OK());
        ASSERT_EQ(prod2->Send(element), Status::OK());
    }

    // The consumer receive 200 elements
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    for (auto &ele : outElements) {
        std::string data(reinterpret_cast<const char *>(ele.ptr), ele.size);
        ASSERT_EQ(data, str);
    }
    outElements.clear();

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(prod2->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
}

TEST_F(PubSubMultiNode1Of2ProducersEnableDataVerificationTest, EnableDataVerification1Of2ProducersSameNode)
{
    // 1 streams: 2 producers -> 1 consumers.
    const std::string stream1 = "testDataVerify1Of2ProdSameNode";

    // Create the 2 producers.
    std::shared_ptr<Producer> prod1; // client1, stream 1, producer 1
    std::shared_ptr<Producer> prod2; // client2, stream 1, producer 2
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod1, defaultProducerConf_));
    datasystem::inject::Set("Mimic.Producer.Old.Version", "1*call()");
    DS_ASSERT_OK(w1Client_->CreateProducer(stream1, prod2, defaultProducerConf_));
    datasystem::inject::Clear("Mimic.Producer.Old.Version");

    // Create the 1 consumers.
    std::shared_ptr<Consumer> con1; // client 3, stream 1, consumer 1
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w3Client_->Subscribe(stream1, config1, con1));

    // Each producer send 100 elements.
    const uint64_t numOfElementPerProducer = 100;
    const uint64_t expectNumOfElementReceive = numOfElementPerProducer * 2;
    const uint64_t elementSize = defaultProducerConf_.pageSize / 10;
    RandomData rand;
    auto str = rand.GetRandomString(elementSize);

    for (uint64_t i = 1; i <= numOfElementPerProducer; i++) {
        Element element(reinterpret_cast<uint8_t *>(&str.front()), elementSize);
        ASSERT_EQ(prod1->Send(element), Status::OK());
        ASSERT_EQ(prod2->Send(element), Status::OK());
    }

    // The consumer receive 200 elements
    std::vector<Element> outElements;
    DS_ASSERT_OK(con1->Receive(expectNumOfElementReceive, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), expectNumOfElementReceive);
    for (auto &ele : outElements) {
        std::string data(reinterpret_cast<const char *>(ele.ptr), ele.size);
        ASSERT_EQ(data, str);
    }
    outElements.clear();

    DS_ASSERT_OK(prod1->Close());
    DS_ASSERT_OK(prod2->Close());
    DS_ASSERT_OK(con1->Close());
    DS_ASSERT_OK(w1Client_->DeleteStream(stream1));
}
}  // namespace st
}  // namespace datasystem

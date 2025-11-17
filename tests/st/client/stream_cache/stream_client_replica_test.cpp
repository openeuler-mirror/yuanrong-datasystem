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
 * Description: Stream client multi replica tests.
 */

#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <utility>

#include <gtest/gtest.h>
#include <unistd.h>

#include "common.h"
#include "common_distributed_ext.h"
#include "client/stream_cache/sc_client_common.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/log/log.h"
#include "datasystem/stream_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

namespace datasystem {
namespace st {
struct StreamEntry {
    std::string name;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consuemr;
};

class StreamReplicaTest : public SCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=1 -shared_memory_size_mb=2048 -node_timeout_s=3 -enable_meta_replica=true -log_monitor=true";
        opts.waitWorkerReady = false;
        SCClientCommon::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

    void InitClients(int count)
    {
        for (int i = 0; i < count; i++) {
            std::shared_ptr<StreamClient> client;
            InitStreamClient(i, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void InitClients(const std::vector<int> &indexes)
    {
        for (auto index : indexes) {
            std::shared_ptr<StreamClient> client;
            InitStreamClient(index, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void BasicTest(const int clientCount = 3)
    {
        int streamCount = 50;
        int maxStreamSize = 10 * 1024 * 1024;
        std::vector<std::string> streamNames;
        std::vector<std::shared_ptr<Producer>> producers;
        std::vector<std::shared_ptr<Consumer>> consumers;
        for (int i = 0; i < streamCount; i++) {
            std::string streamName = GetStringUuid();
            auto pubClient = clients_[i % clientCount];
            auto subClient = clients_[(i + 1) % clientCount];
            std::shared_ptr<Producer> producer;
            ProducerConf conf;
            conf.maxStreamSize = maxStreamSize;
            DS_ASSERT_OK(pubClient->CreateProducer(streamName, producer));
            SubscriptionConfig config;
            std::shared_ptr<Consumer> consumer;
            DS_ASSERT_OK(subClient->Subscribe(streamName, config, consumer));
            streamNames.emplace_back(std::move(streamName));
            producers.emplace_back(std::move(producer));
            consumers.emplace_back(std::move(consumer));
        }

        RandomData random;
        for (auto &s : streamNames) {
            auto client = clients_[random.GetRandomUint32() % clientCount];
            uint64_t producerCount;
            DS_ASSERT_OK(client->QueryGlobalProducersNum(s, producerCount));
            ASSERT_EQ(producerCount, 1ul);
            uint64_t consumerCount;
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(s, consumerCount));
            ASSERT_EQ(consumerCount, 1ul);
        }

        for (auto &p : producers) {
            DS_ASSERT_OK(p->Close());
        }
        for (auto &c : consumers) {
            DS_ASSERT_OK(c->Close());
        }
        for (auto &s : streamNames) {
            auto client = clients_[random.GetRandomUint32() % clientCount];
            DS_ASSERT_OK(client->DeleteStream(s));
        }
    }

    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void CreateStreams(std::vector<StreamEntry> &streams, int streamCount)
    {
        int maxStreamSize = 10 * 1024 * 1024;
        auto clientCount = clients_.size();
        ASSERT_GT(clientCount, 0) << "clients_ not init.";
        for (int i = 0; i < streamCount; i++) {
            std::string streamName = GetStringUuid();
            auto pubClient = clients_[i % clientCount];
            auto subClient = clients_[(i + 1) % clientCount];
            std::shared_ptr<Producer> producer;
            ProducerConf conf;
            conf.maxStreamSize = maxStreamSize;
            DS_ASSERT_OK(pubClient->CreateProducer(streamName, producer));
            SubscriptionConfig config;
            std::shared_ptr<Consumer> consumer;
            DS_ASSERT_OK(subClient->Subscribe(streamName, config, consumer));
            streams.emplace_back(StreamEntry{ streamName, producer, consumer });
        }
    }

    void VerifyStream(std::vector<StreamEntry> &streams)
    {
        auto clientCount = clients_.size();
        ASSERT_GT(clientCount, 0) << "clients_ not init.";
        RandomData random;
        for (auto &entry : streams) {
            auto s = entry.name;
            auto client = clients_[random.GetRandomUint32() % clientCount];
            uint64_t producerCount;
            DS_ASSERT_OK(client->QueryGlobalProducersNum(s, producerCount));
            ASSERT_EQ(producerCount, 1ul);
            uint64_t consumerCount;
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(s, consumerCount));
            ASSERT_EQ(consumerCount, 1ul);
        }
    }

    Status SetNotReadyInject(const std::initializer_list<uint32_t> indexes)
    {
        auto injects = { "master.CreateProducer",         "master.CloseProducer", "master.Subscribe",
                         "master.CloseConsumer",          "master.DeleteStream",  "master.QueryGlobalProducersNum",
                         "master.QueryGlobalConsumersNum" };

        for (auto index : indexes) {
            for (auto inject : injects) {
                RETURN_IF_NOT_OK(cluster_->SetInjectAction(WORKER, index, inject, "10%return(K_REPLICA_NOT_READY)"));
            }
            RETURN_IF_NOT_OK(
                cluster_->SetInjectAction(WORKER, index, "master.MigrateSCMetadata", "3*return(K_REPLICA_NOT_READY)"));
        }
        return Status::OK();
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::shared_ptr<StreamClient>> clients_;
};

class StreamReplicaRouterTest : public StreamReplicaTest {};

TEST_F(StreamReplicaRouterTest, DISABLED_LEVEL1_BasicPubSubTest)
{
    // replica layout:
    // worker0: { primary: [worker0], backup: [worker1] }
    // worker1: { primary: [worker1, worker2], backup: []}
    // worker2: { primary: [], backup: [worker0, worker2]}
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    DS_ASSERT_OK(SetNotReadyInject({ 0, 1, 2 }));
    const int clientCount = 3;
    InitClients(clientCount);

    int streamCount = 50;
    std::vector<StreamEntry> streams;
    CreateStreams(streams, streamCount);
    VerifyStream(streams);

    RandomData random;

    for (auto &entry : streams) {
        auto s = entry.name;
        auto client = clients_[random.GetRandomUint32() % clientCount];
        DS_ASSERT_OK(entry.producer->Close());
        DS_ASSERT_OK(entry.consuemr->Close());
        DS_ASSERT_OK(client->DeleteStream(s));
    }
}

TEST_F(StreamReplicaRouterTest, LEVEL2_SubScribeFirstSendReceive)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int streamCount = 5;
    int maxStreamSize = 10 * 1024 * 1024;
    std::vector<std::string> streamNames;
    std::vector<std::shared_ptr<Producer>> producers;
    std::vector<std::shared_ptr<Consumer>> consumers;
    for (int i = 0; i < streamCount; i++) {
        std::string streamName = GetStringUuid();
        auto pubClient = clients_[i % clientCount];
        auto subClient = clients_[(i + 1) % clientCount];
        SubscriptionConfig config;
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(subClient->Subscribe(streamName, config, consumer));
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        conf.maxStreamSize = maxStreamSize;
        DS_ASSERT_OK(pubClient->CreateProducer(streamName, producer));
        std::string data = randomData_.GetPartRandomString(100, 0);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));
        std::vector<Element> elements;
        DS_ASSERT_OK(consumer->Receive(1, 10000, elements));  // wait time is 10000 ms
        ASSERT_EQ(elements.size(), (size_t)1);
        streamNames.emplace_back(std::move(streamName));
        producers.emplace_back(std::move(producer));
        consumers.emplace_back(std::move(consumer));
    }

    RandomData random;
    for (auto &s : streamNames) {
        auto client = clients_[random.GetRandomUint32() % clientCount];
        uint64_t producerCount;
        DS_ASSERT_OK(client->QueryGlobalProducersNum(s, producerCount));
        ASSERT_EQ(producerCount, 1ul);
        uint64_t consumerCount;
        DS_ASSERT_OK(client->QueryGlobalConsumersNum(s, consumerCount));
        ASSERT_EQ(consumerCount, 1ul);
    }
}

TEST_F(StreamReplicaRouterTest, LEVEL2_CloseProducer)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int maxStreamSize = 10 * 1024 * 1024;

    std::string streamName = GetStringUuid();
    auto pubClient = clients_[0];
    auto subClient = clients_[1];
    SubscriptionConfig config;
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(subClient->Subscribe(streamName, config, consumer));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    DS_ASSERT_OK(pubClient->CreateProducer(streamName, producer));
    std::string data = randomData_.GetPartRandomString(100, 0);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> elements;
    DS_ASSERT_OK(consumer->Receive(1, 10000, elements));  // wait time is 10000 ms
    ASSERT_EQ(elements.size(), (size_t)1);

    RandomData random;
    auto client = clients_[random.GetRandomUint32() % clientCount];
    uint64_t producerCount;
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);
    uint64_t consumerCount;
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 1ul);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 0ul);
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 0ul);

    DS_ASSERT_OK(client->DeleteStream(streamName));
}

TEST_F(StreamReplicaRouterTest, LEVEL2_CloseProducerAndConsumer)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int maxStreamSize = 10 * 1024 * 1024;

    std::string streamName = GetStringUuid();
    auto pubClient = clients_[0];
    auto subClient = clients_[1];
    SubscriptionConfig config;
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(subClient->Subscribe(streamName, config, consumer));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    DS_ASSERT_OK(pubClient->CreateProducer(streamName, producer));
    std::string data = randomData_.GetPartRandomString(100, 0);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> elements;
    DS_ASSERT_OK(consumer->Receive(1, 10000, elements));  // wait time is 10000 ms
    ASSERT_EQ(elements.size(), (size_t)1);

    RandomData random;

    auto client = clients_[random.GetRandomUint32() % clientCount];
    uint64_t producerCount;
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);
    uint64_t consumerCount;
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 1ul);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 0ul);
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 0ul);

    DS_ASSERT_OK(client->DeleteStream(streamName));
}

TEST_F(StreamReplicaRouterTest, DISABLED_LEVEL2_TestResetResumeStream)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));

    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);
    int maxStreamSize = 10 * 1024 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    std::shared_ptr<Producer> producer;
    std::string streamName = "testResetAndResumeStream";
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config, consumer));
    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    std::string data = "Hello World 1";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    EXPECT_EQ(outElements.size(), (size_t)1);

    DS_ASSERT_NOT_OK(producer->Send(element));

    DS_ASSERT_NOT_OK(consumer->Receive(1, 0, outElements));
    DS_ASSERT_NOT_OK(consumer->Ack(outElements.back().id));

    DS_ASSERT_OK(producer->Send(element));
    std::vector<Element> elements;
    DS_ASSERT_OK(consumer->Receive(1, 10000, elements));  // wait time is 10000 ms
    ASSERT_EQ(elements.size(), (size_t)1);
}

TEST_F(StreamReplicaRouterTest, LEVEL2_TestMPSC)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // wait for cluster replica to finish init
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);

    const int clientCount = 3;
    InitClients(clientCount);

    int maxStreamSize = 10 * 1024 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    std::shared_ptr<Producer> producer, producer1;
    std::string streamName = "testMPSC";
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer, conf));
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer1, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config, consumer));
    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    std::string data = "Hello World 1";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer1->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(2, 0, outElements));  // idx is 2

    EXPECT_EQ(outElements.size(), (size_t)2);  // idx is 2
    auto client = clients_[1];
    uint64_t producerCount;
    // Number of worker that have at least 1 producer for the stream, if there are 2 or more producer for the same
    // stream on the same worker, that count as 1.
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);
    uint64_t consumerCount;
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 1ul);
}

TEST_F(StreamReplicaRouterTest, LEVEL2_TestMPSCClose)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // wait for cluster replica to finish init
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);

    const int clientCount = 3;
    InitClients(clientCount);
    int maxStreamSize = 10 * 1024 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    std::shared_ptr<Producer> producer, producer1;
    std::string streamName = "testMPSCClose";
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer, conf));
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer1, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config, consumer));
    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    std::string data = "Hello World 1";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer1->Send(element));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(2, 0, outElements));  // idx is 2

    EXPECT_EQ(outElements.size(), (size_t)2);  // idx is 2
    auto client = clients_[1];
    uint64_t producerCount;
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    // Number of worker that have at least 1 producer for the stream, if there are 2 or more producer for the same
    // stream on the same worker, that count as 1.
    ASSERT_EQ(producerCount, 1ul);
    uint64_t consumerCount;
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 1ul);

    DS_ASSERT_OK(producer1->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 0ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 0ul);
}

TEST_F(StreamReplicaRouterTest, LEVEL2_TestSPMC)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // wait for cluster replica to finish init
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);

    const int clientCount = 3;
    InitClients(clientCount);
    int maxStreamSize = 10 * 1024 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    std::shared_ptr<Producer> producer;
    std::string streamName = "testSPMC";
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer, consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config, consumer));
    SubscriptionConfig config1("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config1, consumer1));
    std::vector<std::string> streamNames;
    streamNames.push_back(streamName);

    std::string data = "Hello World 1";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements, outElements1;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements1));
    EXPECT_EQ(outElements.size(), (size_t)1);
    EXPECT_EQ(outElements1.size(), (size_t)1);
    auto client = clients_[1];
    uint64_t producerCount;
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);
    uint64_t consumerCount;
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 2ul);

    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 0ul);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 0ul);
}

TEST_F(StreamReplicaRouterTest, LEVEL2_TestMPMC)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    // wait for cluster replica to finish init
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);

    const int clientCount = 2;
    InitClients(clientCount);
    int maxStreamSize = 10 * 1024 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    std::shared_ptr<Producer> producer;
    std::string streamName = "TestMPMC";
    DS_ASSERT_OK(clients_[0]->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer, consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config, consumer));
    SubscriptionConfig config1("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients_[0]->Subscribe(streamName, config1, consumer1));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, 0, outElements));
    EXPECT_EQ(outElements.size(), (size_t)1);

    auto client = clients_[1];
    uint64_t producerCount, consumerCount;
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    ASSERT_EQ(producerCount, 1ul);
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, 2ul);

    DS_ASSERT_OK(consumer1->Close());
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, producerCount));
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, consumerCount));
    ASSERT_EQ(consumerCount, producerCount);
}

class StreamReplicaScaleTest : public StreamReplicaTest {};

TEST_F(StreamReplicaScaleTest, DISABLED_TestScaleUp)
{
    // Worker exists two primary replica and migrate data to the new node.
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);

    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int streamCount = 50;
    std::vector<StreamEntry> streams;
    CreateStreams(streams, streamCount);
    VerifyStream(streams);

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 3 }, " -inject_actions=master.MigrateSCMetadata:3*return(K_REPLICA_NOT_READY)"));

    const int nodeCount = 4;
    WaitAllNodesJoinIntoHashRing(nodeCount);
    VerifyStream(streams);
}

TEST_F(StreamReplicaScaleTest, DISABLED_TestVoluntaryScaleDown)
{
    // The worker0 switch replica.
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2, 3 }, { { 0, " -inject_actions=worker.ClusterInitFinish:return()" } }));
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);

    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);

    // Get the next worker of worker0 in hashring.
    std::vector<int> indexes = { 0, 1, 2, 3 };
    std::vector<int> clientIndexes = indexes;
    InitWorkersInfoMap(indexes);
    auto nextWorkerOfWorker0 = workersInfo_[0].nextIndex;

    // Get the scale down node index
    indexes.erase(
        std::remove_if(indexes.begin(), indexes.end(), [&](const int &index) { return index == nextWorkerOfWorker0; }),
        indexes.end());

    const int scaleDownIndex = indexes.back();

    // Init client.
    clientIndexes.erase(std::remove_if(clientIndexes.begin(), clientIndexes.end(),
                                       [&](const int &index) { return index == scaleDownIndex; }),
                        clientIndexes.end());
    LOG(INFO) << "init client for index:" << VectorToString(clientIndexes);
    InitClients(clientIndexes);

    int streamCount = 50;
    std::vector<StreamEntry> streams;
    CreateStreams(streams, streamCount);
    VerifyStream(streams);

    VoluntaryScaleDownInject(scaleDownIndex);

    const int nodeCount = 3;
    WaitAllNodesJoinIntoHashRing(nodeCount);
    VerifyStream(streams);
}

TEST_F(StreamReplicaScaleTest, DISABLED_TestScaleDownSourceSwitchReplica)
{
    // test migrate data sorce node already switch replica.
    const int workerIndex = 3;
    const int maxStartTimeoutSec = 30;
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    // The worker3 switch replica then scale down.
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2, 3 }, { { workerIndex, " -inject_actions=worker.ClusterInitFinish:return()" } }, maxStartTimeoutSec,
        FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS)));
    InjectSyncCap({ 0, 1, 2, 3 }, UINT16_MAX, 1);

    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int streamCount = 50;
    std::vector<StreamEntry> streams;
    CreateStreams(streams, streamCount);
    VerifyStream(streams);

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, workerIndex));

    const int nodeCount = 3;
    WaitAllNodesJoinIntoHashRing(nodeCount);
    VerifyStream(streams);
}

TEST_F(StreamReplicaScaleTest, DISABLED_TestScaleDownTargetSwitchReplica)
{
    // test migrate data target node already switch replica.
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    // The worker3 switch replica then scale down.
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1, 2, 3 },
        FormatString(" -node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS)));
    InjectSyncCap({ 0, 1, 2, 3 }, UINT16_MAX, 1);

    // Get the next worker of worker0 in hashring.
    std::vector<int> indexes = { 0, 1, 2, 3 };
    std::vector<int> clientIndexes = indexes;
    InitWorkersInfoMap(indexes);

    int indexOfWorkera, indexOfWorkerb;
    if (!GetTwoWorkerNotBackupEachOther(indexOfWorkera, indexOfWorkerb)) {
        LOG(INFO) << "Cannot find two workers that don't backup each other";
        return;
    }
    LOG(INFO) << "a:" << indexOfWorkera << ", b:" << indexOfWorkerb;

    // // switch replica for workera and workerb
    auto uuidOfWorkera = workersInfo_[indexOfWorkera].uuid;
    auto nextWorkerUuidOfWorkera = workersInfo_[indexOfWorkera].nextUuid;
    LOG(INFO) << "workera:" << uuidOfWorkera << ", " << nextWorkerUuidOfWorkera;

    auto uuidOfWorkerb = workersInfo_[indexOfWorkerb].uuid;
    auto nextWorkerUuidOfWorkerb = workersInfo_[indexOfWorkerb].nextUuid;

    LOG(INFO) << "workerb:" << uuidOfWorkerb << ", " << nextWorkerUuidOfWorkerb;

    DS_ASSERT_OK(SetWaitingElection(uuidOfWorkera, nextWorkerUuidOfWorkera));
    DS_ASSERT_OK(SetWaitingElection(uuidOfWorkerb, nextWorkerUuidOfWorkerb));

    WaitReplicaNotInCurrentNode(indexOfWorkera);
    WaitReplicaNotInCurrentNode(indexOfWorkerb);

    // Init client.
    int timeout = 3;
    sleep(timeout);
    clientIndexes.erase(std::remove_if(clientIndexes.begin(), clientIndexes.end(),
                                       [&](const int &index) { return index == indexOfWorkera; }),
                        clientIndexes.end());
    InitClients(clientIndexes);

    int streamCount = 50;
    std::vector<StreamEntry> streams;
    CreateStreams(streams, streamCount);
    VerifyStream(streams);

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, indexOfWorkera));

    const int nodeCount = 3;
    WaitAllNodesJoinIntoHashRing(nodeCount);
    VerifyStream(streams);
}

class StreamUpdateToReplicaTest : public StreamReplicaRouterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=2048 -node_timeout_s=3 -log_monitor=true";
        opts.waitWorkerReady = false;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};

TEST_F(StreamUpdateToReplicaTest, LEVEL1_TestUpdateToReplicaEnable)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    const int clientCount = 2;
    InitClients(clientCount);
    BasicTest(2);  // index is 2
    ThreadPool pool(2);
    auto fut1 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0)); });
    auto fut2 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1)); });
    fut1.get();
    fut2.get();
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-enable_meta_replica=true"));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, "-enable_meta_replica=true"));
    fut1 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0)); });
    fut2 = pool.Submit([this]() { DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1)); });
    fut1.get();
    fut2.get();

    BasicTest(2);  // index is 2
}

const std::string HOST_IP_PREFIX = "127.0.0.1";
constexpr size_t DEFAULT_WORKER_NUM = 2;
class StreamClientWriteRocksdbTest : public StreamReplicaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=2048 ";
        SCClientCommon::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void InitTestEtcdInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            // We don't check rc here. If table to drop does not exist, it's fine.
            LOG(INFO) << "create table";
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void GetHashOnWorker(size_t workerNum = 2)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        LOG(INFO) << "ring: " << ring.DebugString();
        for (size_t i = 0; i < workerNum; ++i) {
            auto tokens = ring.workers().at(workerAddress_[i]).hash_tokens();
            workerHashValue_.emplace_back(*tokens.begin() - 1);
            LOG(INFO) << FormatString("workerAddress_ %s, workerHashValue_ %d", workerAddress_[i], *tokens.begin() - 1);
        }
        ASSERT_EQ(workerHashValue_.size(), workerNum);
    }

    void GetWorkerUuids()
    {
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ring.ParseFromString(value);
        for (auto worker : ring.workers()) {
            HostPort workerAddr;
            DS_ASSERT_OK(workerAddr.ParseString(worker.first));
            uuidMap_.emplace(std::move(workerAddr), worker.second.worker_uuid());
        }
    }

    void SetWorkerHashInjection(std::vector<uint32_t> injectNode = std::vector<uint32_t>{})
    {
        if (injectNode.size() == 0) {
            for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
                DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
            }
            return;
        }

        for (auto i : injectNode) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
        }
    }

    void UnsetWorkerHashInjection(std::vector<uint32_t> injectNode = std::vector<uint32_t>{})
    {
        if (injectNode.size() == 0) {
            for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
                DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "MurmurHash3"));
            }
            return;
        }

        for (auto i : injectNode) {
            DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "MurmurHash3"));
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<int> indexes,
                                 const std::unordered_map<int, std::string> &workerFlags = {}, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            std::string flags;
            auto iter = workerFlags.find(i);
            if (iter != workerFlags.end()) {
                flags = " " + iter->second;
            }
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort(), flags).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
        InitWorkersInfoMap(indexes);
    }

    void StartWorkerAndWaitReady(std::initializer_list<int> indexes, const std::string &flags, int maxWaitTimeSec = 20)
    {
        std::unordered_map<int, std::string> workerFlags;
        for (auto i : indexes) {
            workerFlags.emplace(i, flags);
        }
        StartWorkerAndWaitReady(indexes, workerFlags, maxWaitTimeSec);
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        InitStreamClient(index, client);
        return Status::OK();
    }

    Status CreateConsumer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          const std::string &subName, std::shared_ptr<Consumer> &consumer)
    {
        SubscriptionConfig config(subName, SubscriptionType::STREAM);
        return client->Subscribe(streamName, config, consumer);
    }

    Status CreateProducer(std::shared_ptr<StreamClient> client, const std::string &streamName,
                          std::shared_ptr<Producer> &producer)
    {
        const int64_t autoFlushTime = 10 * 1000;  // 10s;
        ProducerConf conf = { .delayFlushTime = autoFlushTime,
                              .pageSize = 20 * 1024,
                              .maxStreamSize = TEST_STREAM_SIZE };
        return client->CreateProducer(streamName, producer, conf);
    }

    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
    std::unique_ptr<EtcdStore> db_;
    std::vector<std::string> workerAddress_;
    std::vector<std::string> workerHost_;
    std::vector<uint32_t> workerHashValue_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamClientWriteRocksdbTest, TestNodeRestartWithNoneMode)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=none");
    GetHashOnWorker(DEFAULT_WORKER_NUM);
    SetWorkerHashInjection();
    DS_ASSERT_OK(InitClient(0, client1_));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;
    int index = 0;
    int num = 1;
    std::string streamName = "a_key_hash_to_" + std::to_string(workerHashValue_[index] - num);
    // std::string streamName = "test";
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname1", consumer1));

    std::string str = "hello world!";
    Element element(reinterpret_cast<uint8_t *>((uint8_t *)str.data()), str.length());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    uint32_t expectRecvNum = 10;
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer1->Ack(outElements.back().id));

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    ASSERT_EQ(producer->Send(element).GetCode(), StatusCode::K_SC_ALREADY_CLOSED);
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements).GetCode(), StatusCode::K_SC_ALREADY_CLOSED);
}

TEST_F(StreamClientWriteRocksdbTest, TestNodeRestartWithNoneMode2)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=none");
    GetHashOnWorker(DEFAULT_WORKER_NUM);
    DS_ASSERT_OK(InitClient(0, client1_));
    SetWorkerHashInjection();
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;
    int index = 1;
    LOG(INFO) << "workerHashValue_[index] " << workerHashValue_[index];
    std::string streamName = "a_key_hash_to_" + std::to_string(workerHashValue_[index] - index);
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname1", consumer1));

    std::string str = "hello world!";
    Element element(reinterpret_cast<uint8_t *>((uint8_t *)str.data()), str.length());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    uint32_t expectRecvNum = 10;
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer1->Ack(outElements.back().id));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer2));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname2", consumer2));

    std::string str2 = "hello world 2";
    Element element2(reinterpret_cast<uint8_t *>((uint8_t *)str2.data()), str2.length());
    DS_ASSERT_OK(producer->Send(element2));
    outElements.clear();
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    std::string actualData(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str2, actualData);
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    std::string actualData2(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str2, actualData2);

    std::string str3 = "hello world 3";
    Element element3(reinterpret_cast<uint8_t *>((uint8_t *)str3.data()), str3.length());
    DS_ASSERT_OK(producer2->Send(element3));
    outElements.clear();
    ASSERT_EQ(consumer2->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    std::string actualData3(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str3, actualData3);

    outElements.clear();
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    std::string actualData4(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str3, actualData4);
}

TEST_F(StreamClientWriteRocksdbTest, TestNodeRestartWithNoneMode3)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=none");
    int waitHashSecond = 2;
    sleep(waitHashSecond);
    GetHashOnWorker(DEFAULT_WORKER_NUM);
    SetWorkerHashInjection();
    DS_ASSERT_OK(InitClient(0, client1_));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer1;
    int index = 1;
    LOG(INFO) << "workerHashValue_[index] " << workerHashValue_[index];
    std::string streamName = "a_key_hash_to_" + std::to_string(workerHashValue_[index] - index);
    DS_ASSERT_OK(CreateProducer(client1_, streamName, producer));
    DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname1", consumer1));

    std::string str = "hello world!";
    Element element(reinterpret_cast<uint8_t *>((uint8_t *)str.data()), str.length());
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    uint32_t expectRecvNum = 1;
    ASSERT_EQ(consumer1->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 1ul);
    DS_ASSERT_OK(consumer1->Ack(outElements.back().id));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    DS_ASSERT_OK(InitClient(1, client2_));
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(CreateProducer(client2_, streamName, producer2));
    DS_ASSERT_OK(CreateConsumer(client2_, streamName, "subname3", consumer2));

    std::string str2 = "hello world 2";
    Element element2(reinterpret_cast<uint8_t *>((uint8_t *)str2.data()), str2.length());
    DS_ASSERT_OK(producer->Send(element2));
    outElements.clear();

    ASSERT_EQ(consumer2->Receive(expectRecvNum, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 0ul);

    std::string str3 = "hello world 3";
    Element element3(reinterpret_cast<uint8_t *>((uint8_t *)str3.data()), str3.length());
    DS_ASSERT_OK(producer2->Send(element3));
    outElements.clear();
    int timeoutMs = 1000;
    ASSERT_EQ(consumer2->Receive(expectRecvNum + 1, timeoutMs, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 2ul);
    std::string actualData0(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str3, actualData0);
    std::string actualData(reinterpret_cast<const char *>(outElements[1].ptr), outElements[1].size);
    EXPECT_EQ(str2, actualData);

    outElements.clear();
    ASSERT_EQ(consumer1->Receive(expectRecvNum + 1, timeoutMs, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 2ul);
    std::string actualData2(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    EXPECT_EQ(str2, actualData2);
    std::string actualData3(reinterpret_cast<const char *>(outElements[1].ptr), outElements[1].size);
    EXPECT_EQ(str3, actualData3);
}

}  // namespace st
}  // namespace datasystem

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

#include <utility>

#include <gtest/gtest.h>

#include "common.h"
#include "sc_client_common.h"
#include "client/stream_cache/pub_sub_utils.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/random_data.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class DeleteStreamTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numRpcThreads = 0;
        opts.numWorkers = 2;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestClientInstance();
    }

    void TearDown() override
    {
        client_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTestClientInstance()
    {
        InitStreamClient(0, client_);
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    Status CreateClient(int workerNum, std::shared_ptr<StreamClient> &spClient)
    {
        InitStreamClient(workerNum, spClient);
        return Status::OK();
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

    void ReceiveHelper(std::shared_ptr<Consumer> consumer, size_t numElements)
    {
        Timer timer;
        size_t remaining = numElements;
        int round = 0;
        const int DEFAULT_RETRY_TIME = 10;
        const size_t PER_RECEIVE_NUM = 500;
        const int DEFAULT_WAIT_TIME = 1000;
        while (remaining > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
            std::vector<Element> outElements;
            DS_ASSERT_OK(consumer->Receive(std::max(PER_RECEIVE_NUM, remaining), DEFAULT_WAIT_TIME, outElements));
            LOG(INFO) << "receive num : " << outElements.size() << " ;" << round++;
            if (!outElements.empty()) {
                remaining -= outElements.size();
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            }
        }
    }

    /**
     * @brief Creates a stream client at the given worker num and timeout
     * @param[in] workerNum The worker num to create the stream against
     * @param[in] timeout Timeout for RPC requests
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClient(int workerNum,  int32_t timeout, std::shared_ptr<StreamClient> &spClient)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerNum, workerAddress));
        // Create a client with user defined timeout
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = timeout };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        spClient = std::make_shared<StreamClient>(connectOptions);
        RETURN_IF_NOT_OK(spClient->Init());
        return Status::OK();
    }

    std::shared_ptr<StreamClient> client_ = nullptr;
    ProducerConf defaultProducerConf_;
    SubscriptionConfig config = SubscriptionConfig("sub1", SubscriptionType::STREAM);
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(DeleteStreamTest, CloseProducersAndConsumers)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testCloseProdCon";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client_->DeleteStream(streamName));
}

TEST_F(DeleteStreamTest, ProducerExist)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testProdExist";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    Status rc = client_->DeleteStream(streamName);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(producer->Close(), Status::OK());
    ASSERT_EQ(client_->DeleteStream(streamName), Status::OK());
}

TEST_F(DeleteStreamTest, ConsumerExist)
{
    std::shared_ptr<Consumer> consumer;
    std::string streamName = "testConExist";
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));

    Status rc = client_->DeleteStream(streamName);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
    DS_ASSERT_OK(consumer->Close());
    ASSERT_EQ(client_->DeleteStream(streamName), Status::OK());
}

TEST_F(DeleteStreamTest, MultiSubsExist)
{
    std::shared_ptr<Producer> producer;
    std::string streamName = "testMultiExist";
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer1));

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config2, consumer2));

    ASSERT_EQ(producer->Close(), Status::OK());
    ASSERT_EQ(client_->DeleteStream(streamName).GetCode(), StatusCode::K_RUNTIME_ERROR);

    ASSERT_EQ(consumer1->Close(), Status::OK());
    ASSERT_EQ(client_->DeleteStream(streamName).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(consumer2->Close(), Status::OK());
    ASSERT_EQ(client_->DeleteStream(streamName), Status::OK());
}

TEST_F(DeleteStreamTest, CheckEmptyStreamAfterDelete)
{
    std::shared_ptr<Producer> producer;
    std::string streamName("testEmptyStreamAfterDelete");
    DS_ASSERT_OK(client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client_->Subscribe(streamName, config, consumer));

    size_t testSize = 1024ul * 1024ul;
    std::vector<uint8_t> writeElement = RandomData().RandomBytes(testSize);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size(), ULONG_MAX);
    DS_ASSERT_OK(producer->Send(element));

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client_->DeleteStream(streamName));
    
    // Check that the buffer is cleared. There should not be any elements
    // to receive after purge.
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe(streamName, config2, consumer));
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), 0);
    DS_ASSERT_OK(consumer->Close());
}

// Testing CreateProducer while DeleteStreams is called
TEST_F(DeleteStreamTest, TestParallelDeleteStreamCreateProducer)
{
    const int timeout = 10000;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, timeout, spClient), Status::OK());
    std::shared_ptr<StreamClient> spClient1;
    ASSERT_EQ(CreateClient(1, timeout, spClient1), Status::OK());
    ThreadPool pool(1);
    // Create a producer and consumer on different nodes
    auto stream_name = "testParallelDelStreamCreateProd";
    ProducerConf prodCfg = { .delayFlushTime = 5, .pageSize = 1 * MB, .maxStreamSize = 2 * MB};
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Producer> producer1;
    spClient->CreateProducer(stream_name, producer, prodCfg);
    producer->Close();
    spClient1->CreateProducer(stream_name, producer1, prodCfg);
    producer1->Close();
    // Inject delay in DeleteStreamContext request in both workers and increase timeout for the request
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "MasterRemoteWorkerSCApi.DelStreamContextBroadcast.sleep",
                                           "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "MasterLocalWorkerSCApi.DelStreamContextBroadcast.sleep",
                                           "1*sleep(5000)"));
    // Make a create producer call while delete stream is active
    auto delFut = pool.Submit([this, &spClient, &stream_name]() {
        // Fails with a timeout
        spClient->DeleteStream(stream_name);
    });
    sleep(1);
    // This should be rejected with a runtime error as delete stream is in progress
    ASSERT_EQ(spClient1->CreateProducer(stream_name, producer, prodCfg).GetCode(),
              StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS);
    delFut.get();
}

TEST_F(DeleteStreamTest, TestParallelDeleteStreamCreateSubscriber)
{
    const int timeout = 10000;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, timeout, spClient), Status::OK());
    std::shared_ptr<StreamClient> spClient1;
    ASSERT_EQ(CreateClient(1, timeout, spClient1), Status::OK());
    ThreadPool pool(1);
    // Create a producer and consumer on different nodes
    auto stream_name = "testParallelDelStreamCreateCon";
    ProducerConf prodCfg = { .delayFlushTime = 5, .pageSize = 1 * MB, .maxStreamSize = 2 * MB};
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Producer> producer1;
    spClient->CreateProducer(stream_name, producer, prodCfg);
    producer->Close();
    spClient1->CreateProducer(stream_name, producer1, prodCfg);
    producer1->Close();
    // Inject delay in DeleteStreamContext request in both workers and increase timeout for the request
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "MasterRemoteWorkerSCApi.DelStreamContextBroadcast.sleep",
                                           "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "MasterLocalWorkerSCApi.DelStreamContextBroadcast.sleep",
                                           "1*sleep(5000)"));
    // Make a create producer call while delete stream is active
    auto delFut = pool.Submit([this, &spClient, &stream_name]() {
        // Fails with a timeout
        spClient->DeleteStream(stream_name);
    });
    sleep(1);
    std::shared_ptr<Consumer> consumer;
    // This should be rejected with a runtime error as delete stream is in progress
    ASSERT_EQ(spClient1->Subscribe(stream_name, config, consumer).GetCode(),
              StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS);
    delFut.get();
}

// Testing AutoDelete retry while CreateProducer is done
TEST_F(DeleteStreamTest, TestParallelCreateProducerDeleteStream)
{
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    std::shared_ptr<StreamClient> spClient1;
    ASSERT_EQ(CreateClient(1, spClient1), Status::OK());
    ThreadPool pool(1);
    // Create a producer and consumer on different nodes
    auto stream_name = "testParallelCreateProdDelStream";
    ProducerConf prodCfg = { .delayFlushTime = 5,
                             .pageSize = 1 * MB,
                             .maxStreamSize = 2 * MB};
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Producer> producer1;
    spClient->CreateProducer(stream_name, producer, prodCfg);
    producer->Close();
    spClient1->CreateProducer(stream_name, producer1, prodCfg);
    producer1->Close();
    // Inject delay in DeleteStream so that it waits before checking in master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(7000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(7000)"));
    // As create producer comes in between check fails
    auto delFut = pool.Submit([this, &spClient, &stream_name]() {
        // Fails with a timeout
        ASSERT_EQ(spClient->DeleteStream(stream_name).GetCode(), StatusCode::K_SC_STREAM_IN_USE);
    });
    usleep(4000);
    // This should be rejected with a runtime error as delete stream is in progress
    ASSERT_EQ(spClient1->CreateProducer(stream_name, producer, prodCfg).GetCode(),
              StatusCode::K_OK);
    delFut.get();
}

TEST_F(DeleteStreamTest, LEVEL2_TestDeleteLongTimeout)
{
    // Request should not timeout if client timeout is set to 10s and master takes more time

    // set timeout to 10 mins
    std::shared_ptr<StreamClient> client1;
    const int32_t timeoutMs = 1000 * 60 * 10;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    // Make master wait for 1 min and it should not timeout
    // We actually dont know who is the master so inject in both
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(60000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(60000)"));

    // This request should not timeout as client timeout is 10 mins.
    DS_ASSERT_OK(client1->CreateProducer("testDelLongTimeout", producer, conf));
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(client1->DeleteStream("testDelLongTimeout"));
}

TEST_F(DeleteStreamTest, TestDeleteStreamTimingHole1)
{
    // The purpose of the testcase is to test a timing hole in DeleteStream.
    // That is DeleteStreamLocally and DeleteStreamContext can both be skipped,
    // leading to residue in RemoteWorkerManager, etc. and cause other problems.
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(CreateClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(CreateClient(1, client2));
    // Create a producer and consumer on different nodes
    std::string streamName = "testDelStreamTimingHole1";
    const int64_t DEFAULT_PAGE_SIZE = 4 * KB;
    ProducerConf prodCfg = { .delayFlushTime = 5,
                             .pageSize = DEFAULT_PAGE_SIZE,
                             .maxStreamSize = 1 * MB,
                             .autoCleanup = true,
                             .retainForNumConsumers = 1 };
    const size_t DEFAULT_ELEMENT_SIZE = 2000;
    const int ELE_NUM = 10;
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    // Add injection so that auto-delete is triggered after the start of manual-delete
    // but still executed before manual-delete sends out RPC.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(2000)"));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    DS_ASSERT_OK(producer->Close());

    // Delete will not succeed, since K_NOT_FOUND
    DS_ASSERT_NOT_OK(client1->DeleteStream(streamName));

    // Recreate the same stream.
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    DS_ASSERT_OK(producer->Close());
}

TEST_F(DeleteStreamTest, TestDeleteStreamTimingHole2)
{
    // The purpose of the test case is to test a timing hole in DeleteStream.
    // That is DeleteStreamLocally and DeleteStreamContext can both be skipped,
    // leading to residue in RemoteWorkerManager, etc. and cause other problems.
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(CreateClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(CreateClient(1, client2));
    // Create a producer and consumer on different nodes
    std::string streamName = "testDeleteStreamTimingHoleTwo";
    const int64_t DEFAULT_PAGE_SIZE = 4 * KB;
    ProducerConf prodCfg = { .delayFlushTime = 5,
                             .pageSize = DEFAULT_PAGE_SIZE,
                             .maxStreamSize = 1 * MB,
                             .autoCleanup = true,
                             .retainForNumConsumers = 1 };
    const size_t DEFAULT_ELEMENT_SIZE = 2000;
    const int ELE_NUM = 10;
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    // Add injection so that auto-delete is triggered after the start of manual-delete
    // but still executed before manual-delete sends out RPC.
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.sleep", "1*sleep(2000)"));
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client2->CreateProducer(streamName, producer, prodCfg));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    // Give producer enough time to process ack
    sleep(1);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());

    // Delete will not succeed, due to K_NOT_FOUND
    DS_ASSERT_NOT_OK(client1->DeleteStream(streamName));

    // Recreate the same stream.
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    ReceiveHelper(consumer, ELE_NUM);
    DS_ASSERT_OK(producer->Close());
}

class DeleteStreamTimingTest : public DeleteStreamTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numRpcThreads = 0;
        opts.numWorkers = 2;
        DeleteStreamTest::SetClusterSetupOptions(opts);
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 1;
    }

protected:
    const size_t DEFAULT_ELEMENT_SIZE = 2000;
    const int64_t DEFAULT_PAGE_SIZE = 4 * KB;
    const int ELE_NUM = 10, TWO = 2, FIVE = 5;
    uint64_t producersCount, consumersCount;
    ProducerConf prodCfgAutoDel = { .delayFlushTime = 5,
        .pageSize = DEFAULT_PAGE_SIZE,
        .maxStreamSize = 1 * MB,
        .autoCleanup = true,
        .retainForNumConsumers = 1 };
    ProducerConf prodCfg = { .delayFlushTime = 5,
        .pageSize = DEFAULT_PAGE_SIZE,
        .maxStreamSize = 1 * MB,
        .autoCleanup = false,
        .retainForNumConsumers = 1 };
    std::shared_ptr<StreamClient> client1, client2;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
};

TEST_F(DeleteStreamTimingTest, TestDeleteStreamTimingHole3)
{
    // The purpose of the testcase is to test a timing hole in DeleteStream.
    // Running both AutoDelete and Manual Delete at the same time
    DS_ASSERT_OK(CreateClient(0, client1));
    std::string streamName = "testDelStreamTimingHoleThree";
    // Create a producer and consumer
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    // Add injection so that master sends deletestream requests (auto-delete) after
    // but still executed before manual-delete sends out RPC to master to broadcast Delete.
    // slow down the state setting so that both will conflict with each other
    // Sleep manual before api->DeleteStream to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(3000)"));

    // Sleep autodelete after sending broadcast to worker
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.SentReqs", "1*sleep(5000)"));

    // Create a producer and consumer
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    // Give producer enough time to process ack
    sleep(1);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    // Delete will not succeed, since AutoDelete deletes first.
    DS_ASSERT_NOT_OK(client1->DeleteStream(streamName));
    LOG(INFO) << "Deleted stream";
    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 0ul);
    ASSERT_EQ(consumersCount, 0ul);
    sleep(FIVE);
    // Check logs for "Setting Active State"
}

TEST_F(DeleteStreamTimingTest, TestDeleteStreamTimingHole4)
{
    // The purpose of the testcase is to test running both Auto Delete and CreateProducer
    // at the same time. AutoDelete is not finished when CreateProducer starts.
    std::string streamName = "TestDelStreamTimingHoleFour";
    DS_ASSERT_OK(CreateClient(0, client1));
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Sleep autodelete before sending broadcast to worker
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.SendReqs", "1*sleep(5000)"));

    // Create a producer and consumer
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    // Give producer enough time to process ack
    sleep(1);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    sleep(1);
    // AutoDelete is running, so createproducer does not succeed until delete is finished
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));
    sleep(FIVE);
    // Delete finished
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));

    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 1ul);
    ASSERT_EQ(consumersCount, 0ul);
}

TEST_F(DeleteStreamTimingTest, TestDeleteStreamTimingHole5)
{
    // The purpose of the testcase is to test running 2 Manual Delete (no AutoDelete),
    // and CreateProducer at the same time. Manual Deletes ignore the
    // delete-in-progress, while CreateProducer runs.
    DS_ASSERT_OK(CreateClient(0, client1));
    DS_ASSERT_OK(CreateClient(1, client2));
    std::string streamName = "TestDelStreamTimingHole5";
    ThreadPool pool(TWO);
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Sleep manual before api->DeleteStream to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(3000)"));

    // Sleep autodelete after sending broadcast to worker
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.SentReqs", "1*sleep(5000)"));

    // Create a producer and consumer
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    // Give producer enough time to process ack
    sleep(1);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    sleep(1);
    bool a, b;
    auto delFut = pool.Submit([this, &a, streamName]() {
        Status rc = client1->DeleteStream(streamName);
        a = rc.IsOk();
        LOG(INFO) << FormatString("DeleteStream on W0 %ssuccessful", (a ? "" : "un"));
    });
    auto delFut2 = pool.Submit([this, &b, streamName]() {
        Status rc = client2->DeleteStream(streamName);
        b = rc.IsOk();
        LOG(INFO) << FormatString("DeleteStream on W1 %ssuccessful", (b ? "" : "un"));
    });
    sleep(1);
    // AutoDelete is running, so createproducer does not succeed until delete is finished
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    delFut.get();
    delFut2.get();
    ASSERT_TRUE(a != b && (a || b));
    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 0ul);
    ASSERT_EQ(consumersCount, 0ul);
    sleep(FIVE);
    // Delete finished
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));

    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 1ul);
    ASSERT_EQ(consumersCount, 0ul);
}

TEST_F(DeleteStreamTimingTest, LEVEL2_TestDeleteStreamTimingHole6)
{
    // The purpose of the testcase is to test running CreateProducer during
    // DeleteStream + AutoDelete but with producers and consumers on both workers.
    std::string streamName = "testDelStreamTimingHole6";
    DS_ASSERT_OK(CreateClient(0, client1));
    DS_ASSERT_OK(CreateClient(1, client2));
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Sleep manual before api->DeleteStream to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(3000)"));

    // Sleep autodelete after sending broadcast to worker
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.SentReqs", "1*sleep(5000)"));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "master.ProcessDeleteStreams", "pause()"));

    // Create a producer and consumer
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));
    DS_ASSERT_OK(client2->Subscribe(streamName, SubscriptionConfig("sub2", SubscriptionType::STREAM), consumer2));
    DS_ASSERT_OK(client2->CreateProducer(streamName, producer2, prodCfgAutoDel));

    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
        SendHelper(producer2, element);
    }
    ReceiveHelper(consumer, TWO * ELE_NUM);
    ReceiveHelper(consumer2, TWO * ELE_NUM);
    // Give producer enough time to process ack
    sleep(TWO);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(consumer2->Close());
    sleep(1);
    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, "master.ProcessDeleteStreams"));
    sleep(1);
    // AutoDelete is running, so createproducer does not succeed until delete is finished
    DS_ASSERT_NOT_OK(client1->DeleteStream(streamName));

    // Without a lock on undo in StreamMetadata, PubIncreaseNode will succeed when StreamManager
    // has not been deleted.
    LOG(INFO) << "Deleted stream";
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));
    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 0ul);
    ASSERT_EQ(consumersCount, 0ul);
    sleep(FIVE);
    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 0ul);
    ASSERT_EQ(consumersCount, 0ul);
    // AutoDelete finished
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfgAutoDel));

    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 1ul);
    ASSERT_EQ(consumersCount, 0ul);
}

TEST_F(DeleteStreamTimingTest, LEVEL1_TestDeleteStreamTimingHole7)
{
    std::string streamName = "testDelStreamTimingHole7";
    // The purpose of the testcase is to test the UndoDeleteStream. We ensure that there is at least one reference to
    // DeleteStream on master, inject an RPC failure so that DeleteStream fails and hits UndoDeleteStream,
    // and then ensure producer can be created successfully.
    int timeoutMs = 15 * 1000;
    DS_ASSERT_OK(CreateClient(0, timeoutMs, client1));
    DS_ASSERT_OK(CreateClient(1, timeoutMs, client2));
    ThreadPool pool(TWO);
    std::string data = RandomData().GetRandomString(DEFAULT_ELEMENT_SIZE);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Sleep manual before api->DeleteStream to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep", "1*sleep(3000)"));

    // Sleep manual after api->DeleteStream to master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "ClientWorkerSCServiceImpl.DeleteStreamHandleSend.sleep", "1*sleep(5000)"));
    
    // Force RPC Timeout by timing out in master
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.sleep",
                                           "2*sleep(2500)"));
    // sleep delete from master before sending broadcast to worker
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1,
                                           "SCMetadataManager.DeleteStream.SendReqs", "1*sleep(10000)"));

    // Create a producer and consumer
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    for (int i = 0; i < ELE_NUM; i++) {
        SendHelper(producer, element);
    }
    ReceiveHelper(consumer, ELE_NUM);
    // Give producer enough time to process ack
    sleep(1);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());

    bool a, b;
    auto delFut = pool.Submit([this, &a, streamName]() {
        ASSERT_EQ(client1->DeleteStream(streamName).GetCode(), K_SC_STREAM_DELETE_IN_PROGRESS);
        LOG(INFO) << "DeleteStream on W0 failed due to Delete-In-Progress";
    });
    auto delFut2 = pool.Submit([this, &b, streamName]() {
        ASSERT_EQ(client2->DeleteStream(streamName).GetCode(), K_RPC_DEADLINE_EXCEEDED);
        LOG(INFO) << "DeleteStream on W1 failed due to RPC";
    });
    sleep(1);
    // Deletes are running, so createproducer does not succeed until delete is finished
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    delFut.get();
    delFut2.get();
    // Both deletes failed, we should now be able to create because delete master fails
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, prodCfg));
    DS_ASSERT_OK(client1->QueryGlobalProducersNum(streamName, producersCount));
    DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamName, consumersCount));
    ASSERT_EQ(producersCount, 1ul);
    ASSERT_EQ(consumersCount, 0ul);
}

}  // namespace st
}  // namespace datasystem

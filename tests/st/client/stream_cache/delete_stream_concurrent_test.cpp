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
#include <future>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/common/util/thread_pool.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
constexpr int K_TEN = 10;
class DeleteStreamConcurrentTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.numRpcThreads = 0;
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

protected:
    void InitTest()
    {
        HostPort workerAddress1;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress1));
        HostPort workerAddress2;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress2));
        HostPort workerAddress3;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(2, workerAddress3));
        LOG(INFO) << FormatString("\n Worker1: <%s>\n Worker2: <%s>\n Worker3: <%s>", workerAddress1.ToString(),
                                  workerAddress2.ToString(), workerAddress3.ToString());
        InitStreamClient(0, client1_);
        InitStreamClient(1, client2_);
        InitStreamClient(2, client3_); // worker index is 2
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

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

    std::shared_ptr<StreamClient> client1_ = nullptr;
    std::shared_ptr<StreamClient> client2_ = nullptr;
    std::shared_ptr<StreamClient> client3_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(DeleteStreamConcurrentTest, DeleteBySequence)
{
    std::string stream1("DeleteBySequence");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);
    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));

    std::shared_ptr<Producer> node3Producer1;
    DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node3Consumer1;
    DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));

    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(node1Consumer1->Close());
    DS_ASSERT_NOT_OK(client1_->DeleteStream(stream1));

    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(node2Consumer1->Close());
    DS_ASSERT_NOT_OK(client2_->DeleteStream(stream1));

    DS_ASSERT_OK(node3Producer1->Close());
    DS_ASSERT_OK(node3Consumer1->Close());
    // Now close consumer sends async update topo notifications
    // So, we need to wait sometime before doing delete stream
    DS_ASSERT_OK(TryAndDeleteStream(client3_, stream1));
}

TEST_F(DeleteStreamConcurrentTest, DeleteFromUnrelatedNode)
{
    std::string stream1("DeleteFromUnrelatedNode");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    std::promise<void> promise1;
    std::promise<void> promise2;
    std::future<void> future1 = promise1.get_future();
    std::future<void> future2 = promise2.get_future();
    ThreadPool pool(3);
    pool.Submit([this, stream1, &config1, &promise1]() {
        std::shared_ptr<Producer> n1p1;
        DS_ASSERT_OK(client1_->CreateProducer(stream1, n1p1, defaultProducerConf_));
        std::shared_ptr<Consumer> n1c1;
        DS_ASSERT_OK(client1_->Subscribe(stream1, config1, n1c1));
        DS_ASSERT_OK(n1p1->Close());
        DS_ASSERT_OK(n1c1->Close());
        promise1.set_value();
    });
    pool.Submit([this, stream1, &config2, &promise2]() {
        std::shared_ptr<Producer> n2p1;
        DS_ASSERT_OK(client1_->CreateProducer(stream1, n2p1, defaultProducerConf_));
        std::shared_ptr<Consumer> n2c1;
        DS_ASSERT_OK(client1_->Subscribe(stream1, config2, n2c1));
        DS_ASSERT_OK(n2p1->Close());
        DS_ASSERT_OK(n2c1->Close());
        promise2.set_value();
    });
    pool.Submit([this, stream1, &future1, &future2]() {
        future1.get();
        future2.get();
        DS_ASSERT_OK(client3_->DeleteStream(stream1));
    });
}

TEST_F(DeleteStreamConcurrentTest, ConcurrentDelete)
{
    std::string stream1("ConcurrentDelete");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));

    DS_ASSERT_OK(node1Producer1->Close());
    DS_ASSERT_OK(node1Consumer1->Close());
    DS_ASSERT_OK(node2Producer1->Close());
    DS_ASSERT_OK(node2Consumer1->Close());
    {
        ThreadPool pool(3);
        pool.Submit([this, stream1]() { client1_->DeleteStream(stream1); });
        pool.Submit([this, stream1]() { client2_->DeleteStream(stream1); });
    }
}

TEST_F(DeleteStreamConcurrentTest, DeleteWhenSub)
{
    std::string stream1("DeleteWhenSub");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);

    std::shared_ptr<Producer> node1Producer1;
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node1Consumer1;
    DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));

    std::shared_ptr<Producer> node2Producer1;
    DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
    std::shared_ptr<Consumer> node2Consumer1;
    DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));

    {
        ThreadPool pool(2);
        pool.Submit([this, stream1, &node1Producer1, &node1Consumer1]() {
            DS_ASSERT_OK(node1Producer1->Close());
            DS_ASSERT_OK(node1Consumer1->Close());
            LOG(INFO) << "Thread:<client2>, State:<begin>";
            client2_->DeleteStream(stream1);
            LOG(INFO) << "Thread:<client2>, State:<success>";
        });
        pool.Submit([this, stream1, &node2Producer1, &node2Consumer1]() {
            DS_ASSERT_OK(node2Producer1->Close());
            DS_ASSERT_OK(node2Consumer1->Close());
            LOG(INFO) << "Thread:<client3>, State:<begin>";
            client3_->DeleteStream(stream1);
            LOG(INFO) << "Thread:<client3>, State:<success>";
        });
    }
}

TEST_F(DeleteStreamConcurrentTest, DeleteWhenPubSub)
{
    std::string stream1("DeleteWhenPubSub");
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    SubscriptionConfig config3("sub3", SubscriptionType::STREAM);

    {
        ThreadPool pool(3);
        pool.Submit([this, stream1, config1]() {
            std::shared_ptr<Producer> node1Producer1;
            DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
            std::shared_ptr<Consumer> node1Consumer1;
            DS_ASSERT_OK(client1_->Subscribe(stream1, config1, node1Consumer1));
            DS_ASSERT_OK(node1Producer1->Close());
            DS_ASSERT_OK(node1Consumer1->Close());
            client3_->DeleteStream(stream1);
        });
        pool.Submit([this, stream1, config2]() {
            std::shared_ptr<Producer> node2Producer1;
            DS_ASSERT_OK(client2_->CreateProducer(stream1, node2Producer1, defaultProducerConf_));
            std::shared_ptr<Consumer> node2Consumer1;
            DS_ASSERT_OK(client2_->Subscribe(stream1, config2, node2Consumer1));
            DS_ASSERT_OK(node2Producer1->Close());
            DS_ASSERT_OK(node2Consumer1->Close());
            client1_->DeleteStream(stream1);
        });
        pool.Submit([this, stream1, config3]() {
            std::shared_ptr<Producer> node3Producer1;
            DS_ASSERT_OK(client3_->CreateProducer(stream1, node3Producer1, defaultProducerConf_));
            std::shared_ptr<Consumer> node3Consumer1;
            DS_ASSERT_OK(client3_->Subscribe(stream1, config3, node3Consumer1));
            DS_ASSERT_OK(node3Producer1->Close());
            DS_ASSERT_OK(node3Consumer1->Close());
            client2_->DeleteStream(stream1);
        });
    }
}

TEST_F(DeleteStreamConcurrentTest, ParallelDeleteCreate)
{
    std::string stream1("ParallelDeleteCreate");
    {
        ThreadPool pool(2);
        std::shared_ptr<Producer> node1Producer1;
        DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
        DS_ASSERT_OK(node1Producer1->Close());
        // Inject delay in DeleteStream so that it waits before checking in master
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                               "ClientWorkerSCServiceImpl.DeleteStreamLocally.sleep",
                                               "1*sleep(7000)"));
        pool.Submit([this, stream1]() {
            client1_->DeleteStream(stream1);
        });
        pool.Submit([this, stream1]() {
            sleep(1);
            std::shared_ptr<Producer> node1Producer2;
            ASSERT_EQ(client1_->CreateProducer(stream1, node1Producer2, defaultProducerConf_).GetCode(),
                      StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS);
        });
    }
}

TEST_F(DeleteStreamConcurrentTest, TestAutoDeleteWhileCloseConsumerNotification)
{
    // 1. Slowdown the async notifications to 3 secs -> So that notifications are sent slowly
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.SendPendingNotification", "1*sleep(3000)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "master.SendPendingNotification", "1*sleep(3000)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 2, "master.SendPendingNotification", "1*sleep(3000)"));

    // 2. Create a producer and consumer on different nodes -> So that they send notifications
    auto streamName = "AutoDeleteWhileCloseCon";
    defaultProducerConf_.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2_->Subscribe(streamName, config, consumer));

    // 3. Close consumer before producer -> This will introduce async notifications
    consumer->Close();
    producer->Close();

    // Wait for Auto delete to kick in
    sleep(K_TWO);

    // Auto delete wont be deleting the stream as it has pending notifications
    // We use delete stream API to check if stream still exists
    // This call should fail with the error K_SC_STREAM_NOTIFICATION_PENDING
    ASSERT_EQ(client1_->DeleteStream(streamName).GetCode(), StatusCode::K_SC_STREAM_NOTIFICATION_PENDING);

    // Time between auto delete retries is 10 secs
    // Sleep for 10 secs, so that auto delete kicks in again
    sleep(K_TEN);

    // Retry should be successful as notification take 3.x secs (because of introduced delay)
    // Auto delete must have deleted this stream and we should get K_NOT_FOUND on manual delete
    ASSERT_EQ(client1_->DeleteStream(streamName).GetCode(), StatusCode::K_NOT_FOUND);
}


TEST_F(DeleteStreamConcurrentTest, ParallelDeleteReset)
{
    std::string stream1("ParallelDeleteReset");
    {
        ThreadPool pool(2);
        std::shared_ptr<Producer> node1Producer1;
        DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer1, defaultProducerConf_));
        DS_ASSERT_OK(node1Producer1->Close());
        // Inject delay in DeleteStream so that it waits before checking in master
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                               "ClientWorkerSCServiceImpl.DeleteStreamLocally.sleep",
                                               "1*sleep(7000)"));
        pool.Submit([this, stream1]() {
            client1_->DeleteStream(stream1);
        });
        pool.Submit([this, stream1]() {
            std::vector<std::string> streamNames;
            streamNames.push_back(stream1);
            sleep(1);
        });
    }
}

TEST_F(DeleteStreamConcurrentTest, ConcurrentDeleteCheckErrorCodes)
{
    std::string stream1("ConcurrentDeleteCheckErrorCodes");
    std::shared_ptr<Producer> node1Producer;
    ThreadPool pool(2);
    DS_ASSERT_OK(client1_->CreateProducer(stream1, node1Producer, defaultProducerConf_));
    DS_ASSERT_OK(node1Producer->Close());
    // When 2 delete operations come in parallel
    // 3 types of return codes are possible:
    //      OK - if the call is successful
    //      K_SC_STREAM_DELETE_IN_PROGRESS - Another call is still in progress
    //      K_NOT_FOUND - Stream already deleted
    pool.Submit([this, stream1]() {
        Status rc = client1_->DeleteStream(stream1);
        EXPECT_TRUE(rc.IsOk() ||
                    rc.GetCode() == StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS ||
                    rc.GetCode() == StatusCode::K_NOT_FOUND);
    });
    pool.Submit([this, stream1] () {
        Status rc = client2_->DeleteStream(stream1);
        EXPECT_TRUE(rc.IsOk() ||
                    rc.GetCode() == StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS ||
                    rc.GetCode() == StatusCode::K_NOT_FOUND);
    });
}
}  // namespace st
}  // namespace datasystem

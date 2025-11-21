/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Unit test for multi-tenant
 */

#include <vector>
#include <gtest/gtest.h>
#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
const uint32_t WORKER_NUM = 6;

class StreamMultiTenant: public SCClientCommon {
public:
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
        const int64_t pageSize = 4 * 1024; // The size of page is 4096 bytes
        ProducerConf conf = { .delayFlushTime = autoFlushTime, .pageSize = pageSize,
            .maxStreamSize = TEST_STREAM_SIZE };
        return client->CreateProducer(streamName, producer, conf);
    }

    using VECPRODUCER = std::vector<std::shared_ptr<Consumer>>;
    void SendAndReceiveData(const size_t dataSize, std::shared_ptr<Producer> producer,
        VECPRODUCER receiveCon, VECPRODUCER unReceiveCon)
    {
        std::string data = RandomData().GetRandomString(dataSize);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));
        sleep(1);

        for (auto consumer: receiveCon) {
            std::vector<Element> outElements;
            ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
            ASSERT_EQ(memcmp(outElements[0].ptr, data.c_str(), outElements[0].size), 0);
        }

        for (auto consumer: unReceiveCon) {
            std::vector<Element> outElements;
            ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
            EXPECT_EQ(outElements.size(), (size_t)0);
        }
    }

    void QueryProducerAndConsumer(std::shared_ptr<StreamClient> queryProducer, uint64_t producerCnt,
        std::shared_ptr<StreamClient> queryConsumer, uint64_t consumerCnt, std::string streamName)
    {
        uint64_t producersCount = 0;
        uint64_t consumersCount = 0;
        DS_ASSERT_OK(queryProducer->QueryGlobalProducersNum(streamName, producersCount));
        ASSERT_EQ(producersCount, producerCnt);
        DS_ASSERT_OK(queryConsumer->QueryGlobalConsumersNum(streamName, consumersCount));
        ASSERT_EQ(consumersCount, consumerCnt);
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        client3_.reset();
        client4_.reset();
        client5_.reset();
        ExternalClusterTest::TearDown();
    }

    void IdenticalStreamNameDataIsolation(std::string streamName)
    {
        std::shared_ptr<Producer> client0Pro, client2Pro, client3Pro;
        std::shared_ptr<Consumer> client1Con, client4Con, client5Con;
        DS_ASSERT_OK(CreateProducer(client0_, streamName, client0Pro));
        DS_ASSERT_OK(CreateProducer(client2_, streamName, client2Pro));
        DS_ASSERT_OK(CreateProducer(client3_, streamName, client3Pro));
        DS_ASSERT_OK(CreateConsumer(client1_, streamName, "subname1", client1Con));
        DS_ASSERT_OK(CreateConsumer(client4_, streamName, "subname2", client4Con));
        DS_ASSERT_OK(CreateConsumer(client5_, streamName, "subname3", client5Con));

        // 1. Send small and large date by different tenant client with the same stream name.
        const size_t smallElementSize = 10;
        SendAndReceiveData(smallElementSize, client0Pro, VECPRODUCER{client1Con}, VECPRODUCER{client4Con, client5Con});
        SendAndReceiveData(smallElementSize, client2Pro, VECPRODUCER{client4Con, client5Con}, VECPRODUCER{client1Con});

        // 2. Query the num of the producer and consumer by stream name.
        QueryProducerAndConsumer(client0_, 1, client1_, 1, streamName); // Expect 1 producer and 1 consumer
        QueryProducerAndConsumer(client2_, 2, client3_, 2, streamName); // Expect 2 producers and 2 consumers

        // 3. Query the num after closing producer and consumer
        DS_ASSERT_OK(client2Pro->Close());
        DS_ASSERT_OK(client4Con->Close());
        QueryProducerAndConsumer(client0_, 1, client1_, 1, streamName);
        QueryProducerAndConsumer(client3_, 1, client5_, 1, streamName);

        // 4. Send date and query num after deleting stream
        DS_ASSERT_OK(client0Pro->Close());
        DS_ASSERT_OK(client1Con->Close());
        DS_ASSERT_OK(client0_->DeleteStream(streamName));
        QueryProducerAndConsumer(client0_, 0, client1_, 0, streamName); // Expect no producer and consumer
        QueryProducerAndConsumer(client3_, 1, client5_, 1, streamName); // Expect 1 producer and 1 consumer
        SendAndReceiveData(smallElementSize, client3Pro, VECPRODUCER{client5Con}, VECPRODUCER{});
        std::string data = RandomData().GetRandomString(10);
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_NOT_OK(client0Pro->Send(element));
    }

    void DifferentStreamNameDataIsolation(std::string streamName)
    {
        std::string streamName1 = streamName + std::to_string(1);
        std::string streamName2 =  streamName + std::to_string(2);
        std::shared_ptr<Producer> client0Pro1, client0Pro2;
        std::shared_ptr<Consumer> client1Con1, client1Con2;
        DS_ASSERT_OK(CreateProducer(client0_, streamName1, client0Pro1));
        DS_ASSERT_OK(CreateProducer(client0_, streamName2, client0Pro2));
        DS_ASSERT_OK(CreateConsumer(client1_, streamName1, "subname", client1Con1));
        DS_ASSERT_OK(CreateConsumer(client1_, streamName2, "subname", client1Con2));

        // 1. Send small and large date by different tenant client with the different stream name.
        const size_t smallElementSize = 10;
        SendAndReceiveData(smallElementSize, client0Pro1, VECPRODUCER{client1Con1}, VECPRODUCER{client1Con2});

        // 2. Query the num of the producer and consumer by stream name.
        QueryProducerAndConsumer(client0_, 1, client1_, 1, streamName1); // Expect 1 producer and 1 consumer
        QueryProducerAndConsumer(client1_, 1, client0_, 1, streamName2); // Expect 1 producer and 1 consumer

        // 3. Query the num after closing producer and consumer
        DS_ASSERT_OK(client0Pro1->Close());
        DS_ASSERT_OK(client1Con1->Close());
        DS_ASSERT_OK(client0_->DeleteStream(streamName1));
        QueryProducerAndConsumer(client0_, 0, client1_, 0, streamName1); // Expect no producer and consumer
        QueryProducerAndConsumer(client1_, 1, client0_, 1, streamName2); // Expect 1 producer and 1 consumer

        // 4. Query the num after closing producer and consumer
        DS_ASSERT_OK(client0Pro2->Close());
        DS_ASSERT_OK(client1Con2->Close());
        DS_ASSERT_OK(client1_->DeleteStream(streamName2));
        QueryProducerAndConsumer(client0_, 0, client1_, 0, streamName1); // Expect no producer and consumer
        QueryProducerAndConsumer(client1_, 0, client0_, 0, streamName2); // Expect no producer and consumer
    }

protected:
    std::shared_ptr<StreamClient> client0_;
    std::shared_ptr<StreamClient> client1_;
    std::shared_ptr<StreamClient> client2_;
    std::shared_ptr<StreamClient> client3_;
    std::shared_ptr<StreamClient> client4_;
    std::shared_ptr<StreamClient> client5_;
};

class StreamMultiTenantTokenAuth : public StreamMultiTenant {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.workerGflagParams = " -authorization_enable=true -v=2 "
            "-page_size=4096 -shared_memory_size_mb=10240 ";
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        for (size_t i = 0; i < WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
            if (i <= 1) {
                DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.auth", "100*return(Token, TenantId1)"));
            } else {
                DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.auth", "100*return(Token, TenantId2)"));
            }
        }

        DS_ASSERT_OK(InitClient(0, client0_)); // Init client to worker 0
        DS_ASSERT_OK(InitClient(1, client1_)); // Init client to worker 1
        DS_ASSERT_OK(InitClient(2, client2_)); // Init client to worker 2
        DS_ASSERT_OK(InitClient(3, client3_)); // Init client to worker 3
        DS_ASSERT_OK(InitClient(4, client4_)); // Init client to worker 4
        DS_ASSERT_OK(InitClient(5, client5_)); // Init client to worker 5
    }

private:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port() };
        if (index <= 1) {
            connectOptions.SetAkSkAuth(accessKey_, secretKey_, "TenantId1");
        } else {
            connectOptions.SetAkSkAuth(accessKey_, secretKey_, "TenantId2");
        }
        client = std::make_shared<StreamClient>(connectOptions);
        return client->Init();
    }

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamMultiTenantTokenAuth, IdenticalStreamNameDataIsolation)
{
    std::string streamName = "MultiTenantTokenAuthIdenticalName";
    IdenticalStreamNameDataIsolation(streamName);
}

TEST_F(StreamMultiTenantTokenAuth, DISABLED_DifferentStreamNameDataIsolation)
{
    std::string streamName = "MultiTenantTokenAuthDiffName";
    DifferentStreamNameDataIsolation(streamName);
}

class StreamMultiTenantAkSkAuth : public StreamMultiTenant {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.workerGflagParams =  "-page_size=4096 -shared_memory_size_mb=10240 -v=2";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        for (size_t i = 0; i < WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        DS_ASSERT_OK(InitClient(0, client0_, "TenantId1")); // Init client to worker 0
        DS_ASSERT_OK(InitClient(1, client1_, "TenantId1")); // Init client to worker 1
        DS_ASSERT_OK(InitClient(2, client2_, "TenantId2")); // Init client to worker 2
        DS_ASSERT_OK(InitClient(3, client3_, "TenantId2")); // Init client to worker 3
        DS_ASSERT_OK(InitClient(4, client4_, "TenantId2")); // Init client to worker 4
        DS_ASSERT_OK(InitClient(5, client5_, "TenantId2")); // Init client to worker 5
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client, std::string tenantId)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port() };
        connectOptions.SetAkSkAuth(accessKey_, secretKey_, tenantId);
        client = std::make_shared<StreamClient>(connectOptions);
        return client->Init();
    }
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(StreamMultiTenantAkSkAuth, DISABLED_IdenticalStreamNameDataIsolation)
{
    std::string streamName = "MultiTenantAkSkAuthIdenticalName";
    IdenticalStreamNameDataIsolation(streamName);
}

TEST_F(StreamMultiTenantAkSkAuth, DifferentStreamNameDataIsolation)
{
    std::string streamName = "MultiTenantAkSkAuthDiffName";
    DifferentStreamNameDataIsolation(streamName);
}

}  // namespace st
}  // namespace datasystem

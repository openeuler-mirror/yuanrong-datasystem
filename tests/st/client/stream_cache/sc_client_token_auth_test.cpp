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
#include "datasystem/context/context.h"
#include "datasystem/stream/producer.h"

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/common/log/log.h"
#include "sc_client_common.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv_client.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream_client.h"

DS_DECLARE_uint32(page_size);
using namespace datasystem::client::stream_cache;

namespace datasystem {
namespace st {
class SCClientTokenAuthTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = " -authorization_enable=true ";
        opts.numEtcd = 1;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        spClient_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    static void ArrToStr(void *data, size_t sz, std::string &str)
    {
        str.assign(reinterpret_cast<const char *>(data), sz);
    }

    void PubSubElement(std::shared_ptr<StreamClient> spClient1)
    {
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(spClient1->Subscribe("test", config, consumer));
        size_t testSize = 4ul * 1024ul;
        Element element;
        std::vector<uint8_t> writeElement;
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(spClient1->CreateProducer("test", producer, defaultProducerConf_));
        DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
        ASSERT_EQ(producer->Send(element), Status::OK());

        std::vector<Element> outElements;
        ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
        uint64_t conusmerNum, producerNum;
        spClient1->QueryGlobalConsumersNum("test", conusmerNum);
        spClient1->QueryGlobalProducersNum("test", producerNum);

        ASSERT_EQ(conusmerNum, uint64_t(1));
        ASSERT_EQ(conusmerNum, uint64_t(1));
        ASSERT_EQ(outElements.size(), size_t(1));
        ASSERT_EQ(outElements[0].id, size_t(1));
        DS_ASSERT_OK(producer->Close());
        DS_ASSERT_OK(consumer->Close());

        producer.reset();
        consumer.reset();
    }

protected:
    void InitTest()
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(token1,tenant1)"));
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = 60 * 1000,  // 60s
                                          .requestTimeoutMs = 0,
                                          .token = "token1",
                                          .clientPublicKey = "",
                                          .clientPrivateKey = "",
                                          .serverPublicKey = "",
                                          .accessKey = "QTWAOYTTINDUT2QVKYUC",
                                          .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc" };
        spClient_ = std::make_shared<StreamClient>(connectOptions);
        DS_ASSERT_OK(spClient_->Init());
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }
    std::shared_ptr<StreamClient> spClient_ = nullptr;
    ProducerConf defaultProducerConf_;

    Status CreateElement(size_t elementSize, Element &element, std::vector<uint8_t> &writeElement)
    {
        writeElement = RandomData().RandomBytes(elementSize);
        element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
        return Status::OK();
    }
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(SCClientTokenAuthTest, UpdateToken)
{
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer, consumer1;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient_->Subscribe("test", config, consumer));

    size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient_->CreateProducer("test", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    ASSERT_EQ(producer->Send(element), Status::OK());
    SensitiveValue token1("qwer");
    DS_ASSERT_OK(spClient_->UpdateToken(token1));
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_NOT_OK(spClient_->Subscribe("test1", config1, consumer1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(qwer,tenant1)"));
    DS_ASSERT_OK(spClient_->Subscribe("test1", config1, consumer1));
}

TEST_F(SCClientTokenAuthTest, TestTokenAuth)
{
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient_->Subscribe("test", config, consumer));

    size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient_->CreateProducer("test", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    ASSERT_EQ(producer->Send(element), Status::OK());

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);
}

TEST_F(SCClientTokenAuthTest, TestClientWithTenantIds)
{
    // Subscribe before send.
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    ConnectOptions connectOptions;
    connectOptions.host = workerAddress.Host();
    connectOptions.port = workerAddress.Port();
    connectOptions.accessKey = accessKey_;
    connectOptions.secretKey = secretKey_;
    connectOptions.tenantId = "akskTenantId";
    std::shared_ptr<StreamClient> spClient1 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient1->Init());
    std::thread thread1([&spClient1, this] {
        Context::SetTenantId("");
        PubSubElement(spClient1);
    });

    std::thread thread2([&spClient1, this] {
        Context::SetTenantId("tenantId1");
        PubSubElement(spClient1);
    });
    thread1.join();
    thread2.join();
}

TEST_F(SCClientTokenAuthTest, TestClientResetWithTenant)
{
    // Subscribe before send.
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    ConnectOptions connectOptions;
    connectOptions.host = workerAddress.Host();
    connectOptions.port = workerAddress.Port();
    connectOptions.accessKey = accessKey_;
    connectOptions.secretKey = secretKey_;
    connectOptions.tenantId = "";
    std::shared_ptr<StreamClient> spClient1 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient1->Init());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub10", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("test", config, consumer));
    std::thread thread1([&spClient1, this] {
        Context::SetTenantId("tenant2");
        PubSubElement(spClient1);
    });

    std::thread thread2([&spClient1, this] {
        Context::SetTenantId("tenantId1");
        PubSubElement(spClient1);
    });
    thread1.join();
    thread2.join();
}

TEST_F(SCClientTokenAuthTest, TestClientTenant)
{
    // Subscribe before send.
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    ConnectOptions connectOptions;
    connectOptions.host = workerAddress.Host();
    connectOptions.port = workerAddress.Port();
    connectOptions.token = "token1";
    connectOptions.tenantId = "qqqqq";
    std::shared_ptr<StreamClient> spClient1 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient1->Init());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("test", config, consumer));

    size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient1->CreateProducer("test", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
}

TEST_F(SCClientTokenAuthTest, TestCheckoutTenantWhenDeaulfTenantIsEmpty)
{
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "";
    std::string tenantId2 = "tenantId1";
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    ConnectOptions connectOptions;
    connectOptions.host = workerAddress.Host();
    connectOptions.port = workerAddress.Port();
    connectOptions.accessKey = accessKey_;
    connectOptions.secretKey = secretKey_;
    std::shared_ptr<StreamClient> spClient1 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient1->Init());
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient_->Subscribe("test", config, consumer));

    size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient_->CreateProducer("test", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    Context::SetTenantId("tenantId1");
    ASSERT_EQ(producer->Send(element), Status::OK());
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
}

TEST_F(SCClientTokenAuthTest, TestReceiveChangeTenant)
{
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "";
    std::string tenantId2 = "tenantId1";
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    ConnectOptions connectOptions;
    connectOptions.host = workerAddress.Host();
    connectOptions.port = workerAddress.Port();
    connectOptions.accessKey = accessKey_;
    connectOptions.secretKey = secretKey_;
    Context::SetTenantId(tenantId2);
    std::shared_ptr<StreamClient> spClient1 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient1->Init());
    std::shared_ptr<StreamClient> spClient2 = std::make_shared<StreamClient>(connectOptions);
    DS_ASSERT_OK(spClient2->Init());
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::thread t1([&spClient2, &config, &consumer, tenantId2] {
        Context::SetTenantId(tenantId2);
        DS_ASSERT_OK(spClient2->Subscribe("test", config, consumer));
        std::vector<Element> outElements;
        Context::SetTenantId("tenantId2");
        ASSERT_EQ(consumer->Receive(1, 10000, outElements), Status::OK()); // timeout is 10000 ms
        ASSERT_EQ(outElements.size(), size_t(1));
        ASSERT_EQ(outElements[0].id, size_t(1));
    });
    sleep(2); // wait 2 s to send
    size_t testSize = 4ul * 1024ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient1->CreateProducer("test", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    ASSERT_EQ(producer->Send(element), Status::OK());
    t1.join();
}
}  // namespace st
}  // namespace datasystem

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
#include "datasystem/stream_client.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/stream/consumer.h"

DS_DECLARE_uint32(page_size);
using namespace datasystem::client::stream_cache;

namespace datasystem {
namespace st {
class SCClientAkSkAuthTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            " -authorization_enable=true "
            "-yuanrong_iam_url=https://www.example.com";
        opts.numEtcd = 1;
        opts.iamKit = "yuanrong_iam";
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

protected:
    void InitTest()
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.akauth", "return(accessKey,secretKey,tenant1)"));
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddress.Host();
        connectOptions.port = workerAddress.Port();
        connectOptions.connectTimeoutMs = 60000;  // 60000 ms is timeout
        connectOptions.requestTimeoutMs = 0;
        connectOptions.accessKey = "accessKey";
        connectOptions.secretKey = "secretKey";
        connectOptions.tenantId = "tenant1";
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
};

TEST_F(SCClientAkSkAuthTest, LEVEL2_UpdateAkSk)
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
    std::string accessKey1 = "newAk";
    SensitiveValue secretKey1("newSk");
    DS_ASSERT_OK(spClient_->UpdateAkSk(accessKey1, secretKey1));
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_NOT_OK(spClient_->Subscribe("test1", config1, consumer1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.akauth", "return(newAk,newSk,tenant1)"));
    DS_ASSERT_OK(spClient_->Subscribe("test1", config1, consumer1));
}

TEST_F(SCClientAkSkAuthTest, TestAkSkAuth)
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
    std::vector<char> actualData(outElements[0].ptr, outElements[0].ptr + outElements[0].size);
    std::vector<char> data(writeElement.data(), writeElement.data() + writeElement.size());
    EXPECT_EQ(data, actualData);
}
}  // namespace st
}  // namespace datasystem

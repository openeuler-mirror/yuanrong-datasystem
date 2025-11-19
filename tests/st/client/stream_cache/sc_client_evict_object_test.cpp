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
 * Description: Unit test for stream cache
 */
#include <gtest/gtest.h>
#include <memory>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "common/stream_cache/stream_common.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "sc_client_common.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class SCClientEvictObjectTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.numEtcd = 1;
        opts.workerGflagParams = workerConf_;
        opts.injectActions = "worker.Spill.Sync:return()";
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        LOG(INFO) << "start worker for test";
    }
    void TearDown() override
    {
        client_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    void StartClusters()
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

protected:
    void InitTest()
    {
        InitStreamClient(0, client_);
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        ConnectOptions connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port() };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client1_ = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client1_->Init());
        defaultProducerConf_.maxStreamSize = 25 * 1024 * 1024;  // max stream size is 25 * 1024 * 1024
    }
    std::shared_ptr<StreamClient> client_ = nullptr;
    std::shared_ptr<KVClient> client1_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::string workerConf_ = "";
};

TEST_F(SCClientEvictObjectTest, TestEvictObject)
{
    workerConf_ =
        "--shared_memory_size_mb=50 --sc_shm_threshold_percentage=50 --oc_shm_threshold_percentage=100 -v=3 ";
    StartClusters();
    size_t size = 1 * 1024 * 1024;
    std::string prifixKey = "object_data_";
    std::string data = randomData_.GetRandomString(size);
    for (int i = 0; i < 40; i++) {  // object size is 40
        DS_ASSERT_OK(client1_->Set(prifixKey + std::to_string(i), data));
    }
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client_->CreateProducer("stream1", producer, defaultProducerConf_));
    for (int i = 0; i < 20; i++) {  // stream element num is 20
        Element element1(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element1));
    }
}

TEST_F(SCClientEvictObjectTest, TestStreamSizeMax)
{
    workerConf_ =
        "--shared_memory_size_mb=50 --sc_shm_threshold_percentage=50 --oc_shm_threshold_percentage=100 -v=2";
    StartClusters();
    size_t size = 6 * 1024 * 1024;
    std::string prifixKey = "object_data_";
    std::string data = randomData_.GetRandomString(size);
    std::shared_ptr<Producer> producer;
    defaultProducerConf_.maxStreamSize = 50 * 1024 * 1024; // max stream size is 50 * 1024 * 1024
    defaultProducerConf_.pageSize = 10 * 1024 * 1024; // page size is 10 * 1024 * 1024
    DS_ASSERT_OK(client_->CreateProducer("stream1", producer, defaultProducerConf_));
    Status status;
    while (status.IsOk()) {  // stream element num is 10
        Element element1(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        status = producer->Send(element1);
    }
    LOG(INFO) << status.GetMsg();
    ASSERT_TRUE(status.GetMsg().find("Stream cache memory size overflow, maxStreamSize") != std::string::npos);
}

TEST_F(SCClientEvictObjectTest, TestEvictObjNotSpill)
{
    workerConf_ =
        "--shared_memory_size_mb=10 --sc_shm_threshold_percentage=100 --oc_shm_threshold_percentage=50 -v=2";
    StartClusters();
    size_t size = 1 * 1024 * 1024;
    std::string prifixKey = "object_data_";
    std::string data = randomData_.GetRandomString(size);
    for (int i = 0; i < 3; i++) {  // obj size is 3
        DS_ASSERT_OK(client1_->Set(prifixKey + std::to_string(i), data));
    }
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    ProducerConf conf;
    conf.maxStreamSize = 10 * MB; // maxStreamSize is 10 MB
    conf.pageSize = 1 * MB;
    conf.delayFlushTime = 2000; // delay flush time is 2000.
    DS_ASSERT_OK(client_->CreateProducer("stream1", producer, conf));
    SubscriptionConfig config("subName", SubscriptionType::STREAM);
    DS_ASSERT_OK(client_->Subscribe("stream1", config, consumer));
    Status status;
    for (int i = 0; i < 8; i++) {  // stream element num is 8
        Element element1(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element1));
    }
}
}  // namespace st
}  // namespace datasystem

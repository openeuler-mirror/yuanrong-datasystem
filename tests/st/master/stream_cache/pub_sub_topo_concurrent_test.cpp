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
 * Description: Test ObjectMeta Storage basic functions.
 */
#include <memory>
#include <gtest/gtest.h>
#include "common.h"

#include "common/stream_cache/stream_common.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/worker/stream_cache/worker_master_sc_api.h"

namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
using namespace datasystem::client::stream_cache;
class PubSubTopoConcurrentTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 5;
        opts.numEtcd = 1;
        opts.isStreamCacheCase = true;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        clientVector_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTest()
    {
        std::vector<HostPort> workerAddressVector(clientNum_);
        for (int i = 0; i < clientNum_; ++i) {
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddressVector[i]));
            LOG(INFO) << FormatString("Worker%d: <%s>", i, workerAddressVector[i].ToString());
        }

        clientVector_.resize(clientNum_);
        for (size_t i = 0; i < clientVector_.size(); i++) {
            ConnectOptions option;
            option.host = workerAddressVector[i].Host();
            option.port = workerAddressVector[i].Port();
            option.accessKey = accessKey_;
            option.secretKey = secretKey_;
            clientVector_[i] = std::make_unique<StreamClient>(option);
            EXPECT_NE(clientVector_[i], nullptr);
            DS_ASSERT_OK(clientVector_[i]->Init());
        }
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    Status TryAndDeleteStream(std::unique_ptr<StreamClient> &spClient, std::string streamName)
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

    std::vector<std::unique_ptr<StreamClient>> clientVector_;
    uint8_t clientNum_ = 5;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(PubSubTopoConcurrentTest, MNodeMPMC)
{
    std::string stream1("stream1");
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub1", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub2", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub3", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub4", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub5", SubscriptionType::STREAM) };
    {
        ThreadPool pool(clientNum_);
        auto t1 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n0p0;
            std::shared_ptr<Producer> n4p1;
            std::shared_ptr<Consumer> n0c0;
            DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[4]->CreateProducer(stream1, n4p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[0]->Subscribe(stream1, configVector[0], n0c0));
            DS_ASSERT_OK(n0p0->Close());
            DS_ASSERT_OK(n4p1->Close());
            DS_ASSERT_OK(n0c0->Close());
        });
        auto t2 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n1p0;
            std::shared_ptr<Producer> n0p1;
            std::shared_ptr<Consumer> n1c0;
            DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[1]->Subscribe(stream1, configVector[1], n1c0));
            DS_ASSERT_OK(n1p0->Close());
            DS_ASSERT_OK(n0p1->Close());
            DS_ASSERT_OK(n1c0->Close());
        });
        auto t3 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n2p0;
            std::shared_ptr<Producer> n1p1;
            std::shared_ptr<Consumer> n2c0;
            DS_ASSERT_OK(clientVector_[2]->CreateProducer(stream1, n2p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[2]->Subscribe(stream1, configVector[2], n2c0));
            DS_ASSERT_OK(n2p0->Close());
            DS_ASSERT_OK(n1p1->Close());
            DS_ASSERT_OK(n2c0->Close());
        });
        auto t4 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n3p0;
            std::shared_ptr<Producer> n2p1;
            std::shared_ptr<Consumer> n3c0;
            DS_ASSERT_OK(clientVector_[3]->CreateProducer(stream1, n3p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[2]->CreateProducer(stream1, n2p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[3]->Subscribe(stream1, configVector[3], n3c0));
            DS_ASSERT_OK(n3p0->Close());
            DS_ASSERT_OK(n2p1->Close());
            DS_ASSERT_OK(n3c0->Close());
        });
        auto t5 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n4p0;
            std::shared_ptr<Producer> n3p1;
            std::shared_ptr<Consumer> n4c0;
            DS_ASSERT_OK(clientVector_[4]->CreateProducer(stream1, n4p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[3]->CreateProducer(stream1, n3p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[4]->Subscribe(stream1, configVector[4], n4c0));
            DS_ASSERT_OK(n4p0->Close());
            DS_ASSERT_OK(n3p1->Close());
            DS_ASSERT_OK(n4c0->Close());
        });
        t1.wait();
        t2.wait();
        t3.wait();
        t4.wait();
        t5.wait();
        // wait sync notification finish.
        int delaySec = 3;
        sleep(delaySec);
        EXPECT_EQ(clientVector_[1]->DeleteStream(stream1), Status::OK());
    }
}

TEST_F(PubSubTopoConcurrentTest, MNodeMPMCSerial)
{
    std::string stream1("stream1");
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub1", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub2", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub3", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub4", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub5", SubscriptionType::STREAM) };
    std::shared_ptr<Producer> n0p0;
    std::shared_ptr<Producer> n4p1;
    std::shared_ptr<Consumer> n0c0;
    DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p0, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[4]->CreateProducer(stream1, n4p1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[0]->Subscribe(stream1, configVector[0], n0c0));
    DS_ASSERT_OK(n0p0->Close());
    DS_ASSERT_OK(n4p1->Close());
    DS_ASSERT_OK(n0c0->Close());

    std::shared_ptr<Producer> n1p0;
    std::shared_ptr<Producer> n0p1;
    std::shared_ptr<Consumer> n1c0;
    DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p0, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[1]->Subscribe(stream1, configVector[1], n1c0));
    DS_ASSERT_OK(n1p0->Close());
    DS_ASSERT_OK(n0p1->Close());
    DS_ASSERT_OK(n1c0->Close());

    std::shared_ptr<Producer> n2p0;
    std::shared_ptr<Producer> n1p1;
    std::shared_ptr<Consumer> n2c0;
    DS_ASSERT_OK(clientVector_[2]->CreateProducer(stream1, n2p0, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[2]->Subscribe(stream1, configVector[2], n2c0));
    DS_ASSERT_OK(n2p0->Close());
    DS_ASSERT_OK(n1p1->Close());
    DS_ASSERT_OK(n2c0->Close());

    std::shared_ptr<Producer> n3p0;
    std::shared_ptr<Producer> n2p1;
    std::shared_ptr<Consumer> n3c0;
    DS_ASSERT_OK(clientVector_[3]->CreateProducer(stream1, n3p0, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[2]->CreateProducer(stream1, n2p1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[3]->Subscribe(stream1, configVector[3], n3c0));
    DS_ASSERT_OK(n3p0->Close());
    DS_ASSERT_OK(n2p1->Close());
    DS_ASSERT_OK(n3c0->Close());

    std::shared_ptr<Producer> n4p0;
    std::shared_ptr<Producer> n3p1;
    std::shared_ptr<Consumer> n4c0;
    DS_ASSERT_OK(clientVector_[4]->CreateProducer(stream1, n4p0, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[3]->CreateProducer(stream1, n3p1, defaultProducerConf_));
    DS_ASSERT_OK(clientVector_[4]->Subscribe(stream1, configVector[4], n4c0));
    DS_ASSERT_OK(n4p0->Close());
    DS_ASSERT_OK(n3p1->Close());
    DS_ASSERT_OK(n4c0->Close());
    EXPECT_EQ(clientVector_[4]->DeleteStream(stream1), Status::OK());
}

TEST_F(PubSubTopoConcurrentTest, TwoNodeMPMC)
{
    std::string stream1("stream1");
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub0", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub1", SubscriptionType::STREAM) };
    {
        ThreadPool pool(2);
        auto t1 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n0p0;
            std::shared_ptr<Producer> n1p1;
            std::shared_ptr<Consumer> n0c0;
            DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[0]->Subscribe(stream1, configVector[0], n0c0));
            DS_ASSERT_OK(n0p0->Close());
            DS_ASSERT_OK(n1p1->Close());
            DS_ASSERT_OK(n0c0->Close());
        });
        auto t2 = pool.Submit([this, stream1, &configVector]() {
            std::shared_ptr<Producer> n1p0;
            std::shared_ptr<Producer> n0p1;
            std::shared_ptr<Consumer> n1c0;
            DS_ASSERT_OK(clientVector_[1]->CreateProducer(stream1, n1p0, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[0]->CreateProducer(stream1, n0p1, defaultProducerConf_));
            DS_ASSERT_OK(clientVector_[1]->Subscribe(stream1, configVector[1], n1c0));
            DS_ASSERT_OK(n1p0->Close());
            DS_ASSERT_OK(n0p1->Close());
            DS_ASSERT_OK(n1c0->Close());
        });
        t1.wait();
        t2.wait();
        DS_ASSERT_OK(TryAndDeleteStream(clientVector_[1], stream1));
    }
}

TEST_F(PubSubTopoConcurrentTest, DISABLED_MSMNodeMPMC)
{
    std::vector<SubscriptionConfig> configVector = { SubscriptionConfig("sub1", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub2", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub3", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub4", SubscriptionType::STREAM),
                                                     SubscriptionConfig("sub5", SubscriptionType::STREAM) };
    ThreadPool pool(10);
    for (int i = 0; i < 10; ++i) {
        pool.Submit([this, i, &configVector]() {
            std::string streamName = "stream" + std::to_string(i);
            ThreadPool pool1(clientNum_);
            auto t1 = pool1.Submit([this, streamName, &configVector]() {
                std::shared_ptr<Producer> n0p0;
                std::shared_ptr<Producer> n4p1;
                std::shared_ptr<Consumer> n0c0;
                std::shared_ptr<Producer> n0p2;
                DS_ASSERT_OK(clientVector_[0]->CreateProducer(streamName, n0p0, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[4]->CreateProducer(streamName, n4p1, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[0]->Subscribe(streamName, configVector[0], n0c0));
                DS_ASSERT_OK(clientVector_[0]->CreateProducer(streamName, n0p2, defaultProducerConf_));
                DS_ASSERT_OK(n0p0->Close());
                DS_ASSERT_OK(n4p1->Close());
                DS_ASSERT_OK(n0c0->Close());
                DS_ASSERT_OK(n0p2->Close());
            });
            auto t2 = pool1.Submit([this, streamName, &configVector]() {
                std::shared_ptr<Producer> n1p0;
                std::shared_ptr<Producer> n0p1;
                std::shared_ptr<Consumer> n1c0;
                std::shared_ptr<Producer> n1p2;
                DS_ASSERT_OK(clientVector_[1]->CreateProducer(streamName, n1p0, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[0]->CreateProducer(streamName, n0p1, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[1]->Subscribe(streamName, configVector[1], n1c0));
                DS_ASSERT_OK(clientVector_[1]->CreateProducer(streamName, n1p2, defaultProducerConf_));
                DS_ASSERT_OK(n1p0->Close());
                DS_ASSERT_OK(n0p1->Close());
                DS_ASSERT_OK(n1c0->Close());
                DS_ASSERT_OK(n1p2->Close());
            });
            auto t3 = pool1.Submit([this, streamName, &configVector]() {
                std::shared_ptr<Producer> n2p0;
                std::shared_ptr<Producer> n1p1;
                std::shared_ptr<Consumer> n2c0;
                std::shared_ptr<Producer> n2p2;
                DS_ASSERT_OK(clientVector_[2]->CreateProducer(streamName, n2p0, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[1]->CreateProducer(streamName, n1p1, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[2]->Subscribe(streamName, configVector[2], n2c0));
                DS_ASSERT_OK(clientVector_[2]->CreateProducer(streamName, n2p2, defaultProducerConf_));
                DS_ASSERT_OK(n2p0->Close());
                DS_ASSERT_OK(n1p1->Close());
                DS_ASSERT_OK(n2c0->Close());
                DS_ASSERT_OK(n2p2->Close());
            });
            auto t4 = pool1.Submit([this, streamName, &configVector]() {
                std::shared_ptr<Producer> n3p0;
                std::shared_ptr<Producer> n2p1;
                std::shared_ptr<Consumer> n3c0;
                std::shared_ptr<Producer> n3p2;
                DS_ASSERT_OK(clientVector_[3]->CreateProducer(streamName, n3p0, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[2]->CreateProducer(streamName, n2p1, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[3]->Subscribe(streamName, configVector[3], n3c0));
                DS_ASSERT_OK(clientVector_[3]->CreateProducer(streamName, n3p2, defaultProducerConf_));
                DS_ASSERT_OK(n3p0->Close());
                DS_ASSERT_OK(n2p1->Close());
                DS_ASSERT_OK(n3c0->Close());
                DS_ASSERT_OK(n3p2->Close());
            });
            auto t5 = pool1.Submit([this, streamName, &configVector]() {
                std::shared_ptr<Producer> n4p0;
                std::shared_ptr<Producer> n3p1;
                std::shared_ptr<Consumer> n4c0;
                std::shared_ptr<Producer> n4p2;
                DS_ASSERT_OK(clientVector_[4]->CreateProducer(streamName, n4p0, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[3]->CreateProducer(streamName, n3p1, defaultProducerConf_));
                DS_ASSERT_OK(clientVector_[4]->Subscribe(streamName, configVector[4], n4c0));
                DS_ASSERT_OK(clientVector_[4]->CreateProducer(streamName, n4p2, defaultProducerConf_));
                DS_ASSERT_OK(n4p0->Close());
                DS_ASSERT_OK(n3p1->Close());
                DS_ASSERT_OK(n4c0->Close());
                DS_ASSERT_OK(n4p2->Close());
            });
            t1.wait();
            t2.wait();
            t3.wait();
            t4.wait();
            t5.wait();
            EXPECT_EQ(clientVector_[1]->DeleteStream(streamName), Status::OK());
        });
    }
}
}  // namespace st
}  // namespace datasystem

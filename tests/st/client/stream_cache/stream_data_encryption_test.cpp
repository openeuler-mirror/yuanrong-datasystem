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
 * Description: Unit test for stream data encryption support
 */

#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

DS_DECLARE_string(encrypt_kit);
using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class StreamDataEncryptionTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = NUM_WORKERS;
        opts.systemAccessKey = "";
        opts.systemSecretKey = "";
        // Set the encrypted key for stream data encryption
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        defaultConf_.pageSize = DEFAULT_PAGE_SIZE;
        defaultConf_.maxStreamSize = DEFAULT_MAX_STREAM_SIZE;
        // Enable encryptStream by default for test purposes.
        defaultConf_.encryptStream = true;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    /**
     * @brief Creates a stream client at the given worker num
     * @param[in] workerNum The worker num to create the stream against
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClient(int workerNum, std::shared_ptr<StreamClient> &spClient)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerNum, workerAddress));
        ConnectOptions options;
        options.host = workerAddress.Host();
        options.port = workerAddress.Port();
        spClient = std::make_shared<StreamClient>(options);
        RETURN_IF_NOT_OK(spClient->Init());
        return Status::OK();
    }

    Status CreateElement(size_t elementSize, Element &element, std::vector<uint8_t> &writeElement)
    {
        writeElement = RandomData().RandomBytes(elementSize);
        element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
        return Status::OK();
    }

    std::shared_ptr<std::thread> TestSendRecv(std::shared_ptr<Producer> producer, std::shared_ptr<Consumer> consumer)
    {
        return std::make_shared<std::thread>([this, producer, consumer]() {
            const int numElements = 500;
            std::vector<std::vector<uint8_t>> elements(numElements);
            std::thread producerThrd([this, producer, &elements]() {
                const int DEFAULT_SLEEP_TIME = 300;
                auto randomData = RandomData();
                for (int i = 0; i < numElements; i++) {
                    size_t testSize = randomData.GetRandomIndex(10) == 0 ? DEFAULT_BIG_SIZE : DEFAULT_SMALL_SIZE;
                    Element element;
                    int retryLimit = 30;
                    DS_ASSERT_OK(CreateElement(testSize, element, elements[i]));
                    datasystem::Status rc = producer->Send(element);
                    while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                        rc = producer->Send(element);
                    }
                    DS_ASSERT_OK(rc);
                }
            });

            // Receiver should get both small elements and big elements correctly
            std::vector<Element> outElements;
            int received = 0;
            while (received < numElements) {
                DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
                ASSERT_EQ(outElements.size(), 1);
                ASSERT_EQ(outElements[0].size, elements[received].size());
                ASSERT_EQ(memcmp(outElements[0].ptr, elements[received].data(), elements[received].size()), 0);
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
                received++;
            }
            producerThrd.join();
        });
    }

protected:
    const int NUM_WORKERS = 2;
    const int DEFAULT_WAIT_TIME = 10000;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::vector<std::string> validKeys = { "sjdoifjoidjfgfunsjdoifjoidjfgfun", "iusdhfgiojshddagiusdhfgiojshddag" };
    ProducerConf defaultConf_;
    const int DEFAULT_SMALL_SIZE = 10 * KB;
    const int DEFAULT_BIG_SIZE = 60 * KB;
    const int DEFAULT_PAGE_SIZE = 40 * KB;
    const int DEFAULT_MAX_STREAM_SIZE = 50 * MB;
};

TEST_F(StreamDataEncryptionTest, TestStreamSendRecv1)
{
    // Test the basic Stream Data Encryption support.
    // That is, test that remote push functions correctly when
    // ProducerConf and FLAGS_sc_encrypt_secret_key are configured correctly
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create a Producer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient0->CreateProducer("StreamSendRecv1", producer, defaultConf_));

    // Create a Consumer on a different node
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("StreamSendRecv1", config, consumer));

    auto thrd = TestSendRecv(producer, consumer);
    thrd->join();
}

TEST_F(StreamDataEncryptionTest, TestStreamSendRecv2)
{
    // Test that even if worker is set up with FLAGS_sc_encrypt_secret_key,
    // streams can still disable encryption, as this is per stream setting.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create 2 Producers, only one of them enables encryption
    ProducerConf conf = defaultConf_;
    conf.encryptStream = false;
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(spClient0->CreateProducer("StreamSendRecv2_1", producer1, conf));

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(spClient0->CreateProducer("StreamSendRecv2_2", producer2, defaultConf_));

    // Create Consumers on a different node
    std::shared_ptr<Consumer> consumer1;
    SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("StreamSendRecv2_1", config1, consumer1));

    std::shared_ptr<Consumer> consumer2;
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("StreamSendRecv2_2", config2, consumer2));

    auto stream1Thrd = TestSendRecv(producer1, consumer1);
    auto stream2Thrd = TestSendRecv(producer2, consumer2);
    stream1Thrd->join();
    stream2Thrd->join();
}

TEST_F(StreamDataEncryptionTest, TestStreamSendRecv3)
{
    // Test that consumer created before producer also gets the correct stream fields updated.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create Consumer first
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient0->Subscribe("StreamSendRecv3", config, consumer));

    // Create a Producer on a different node
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient1->CreateProducer("StreamSendRecv3", producer, defaultConf_));

    auto thrd = TestSendRecv(producer, consumer);
    thrd->join();
}

TEST_F(StreamDataEncryptionTest, TestSharedPageSendRecv)
{
    // Test that when shared page is enabled, stream encryption is performed correctly.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create Consumer first
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient0->Subscribe("SharedPageSendRecv", config, consumer));

    // Create a Producer on a different node
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = DEFAULT_PAGE_SIZE;
    conf.maxStreamSize = DEFAULT_MAX_STREAM_SIZE;
    // Enable encryptStream and shared page for test purposes.
    conf.encryptStream = true;
    conf.streamMode = StreamMode::SPSC;
    DS_ASSERT_OK(spClient1->CreateProducer("SharedPageSendRecv", producer, conf));

    auto thrd = TestSendRecv(producer, consumer);
    thrd->join();
}

TEST_F(StreamDataEncryptionTest, TestCreateProducerFailure)
{
    // Test that CreateProducer fails if the encryptStream setting mismatch.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create stream producer with encrypt
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(spClient0->CreateProducer("testDiffProdConfig", producer1, defaultConf_));

    ProducerConf conf = defaultConf_;
    conf.encryptStream = false;
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_NOT_OK(spClient1->CreateProducer("testDiffProdConfig", producer2, conf));
}

class StreamDataEncryptionEmptyKeyTest : public StreamDataEncryptionTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = NUM_WORKERS;
        opts.systemAccessKey = "";
        opts.systemSecretKey = "";
        SCClientCommon::SetClusterSetupOptions(opts);
    }
};

TEST_F(StreamDataEncryptionEmptyKeyTest, TestStreamSendRecv)
{
    // Test that if workers are configured with empty FLAGS_sc_encrypt_secret_key,
    // stream data can still be sent correctly.
    // In this case, encryption/decryption is not performed.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create a Producer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient0->CreateProducer("testStreamSendRecv", producer, defaultConf_));

    // Create a Consumer on a different node
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("testStreamSendRecv", config, consumer));

    auto thrd = TestSendRecv(producer, consumer);
    thrd->join();
}

class StreamDataEncryptionPlainTextTest : public StreamDataEncryptionTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = NUM_WORKERS;
        opts.systemAccessKey = "";
        opts.systemSecretKey = "";
        opts.workerGflagParams = "-encrypt_kit=plaintext";
        for (size_t i = 0; i < opts.numWorkers; ++i) {
            auto port = GetFreePort();
            std::string encryptedKey;
            DS_ASSERT_OK(SecretManager::Instance()->Encrypt(validKeys[i], encryptedKey));
            opts.workerSpecifyGflagParams.emplace(
                i, FormatString("-sc_encrypt_secret_key=%s -sc_worker_worker_direct_port=%d", encryptedKey, port));
        }
        opts.isStreamCacheCase = true;
    }
};

TEST_F(StreamDataEncryptionPlainTextTest, TestStreamSendRecv)
{
    // Test that if workers are configured with the default FLAGS_encrypt_kit = "plaintext",
    // stream data can still be sent correctly.
    // In this case, encryption/decryption is not performed.
    // This is guaranteed by send and recv success while keys are set to mismatch.
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create a Producer
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient0->CreateProducer("testDataEncryptPlainText", producer, defaultConf_));

    // Create a Consumer on a different node
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("testDataEncryptPlainText", config, consumer));

    auto thrd = TestSendRecv(producer, consumer);
    thrd->join();
}
}  // namespace st
}  // namespace datasystem

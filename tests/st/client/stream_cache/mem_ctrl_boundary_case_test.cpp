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
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"

namespace datasystem {
namespace st {

constexpr uint64_t SMALL_SHM_SIZE_MB = 2;
constexpr uint64_t ADDITIONAL_INFO_SZ = 100;

class MemCtrlBoundaryCaseTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = workerNum;
        opts.workerGflagParams = " -page_size=" + std::to_string(BIG_PAGE_SIZE)
                                 + " -shared_memory_size_mb=" + std::to_string(SMALL_SHM_SIZE_MB);
        opts.numRpcThreads = 0;
        opts.vLogLevel = 2;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTest();
    }
    
    void TearDown() override
    {
        if (client0_) {
            client0_ = nullptr;
        }
        if (client1_) {
            client1_ = nullptr;
        }
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTest()
    {
        InitStreamClient(0, client0_);
        InitStreamClient(1, client1_);
    }

    Status ProduceFixedSzData(std::shared_ptr<Producer> &producer, const std::string &producerName, uint64_t eleSz,
                              uint64_t eleNum)
    {
        ElementGenerator generator(eleSz, eleSz);
        auto eleList = generator.GenElements(producerName, eleNum, 1);
        for (size_t i = 0; i < eleNum; ++i) {
            LOG(INFO) << FormatString("Element idx:%zu, Element size:%zu", i, eleList[i].size());
            auto mutableData = const_cast<char *>(eleList[i].data());
            Element ele(reinterpret_cast<uint8_t *>(mutableData), eleList[i].size());
            RETURN_IF_NOT_OK(producer->Send(ele));
        }
        return Status::OK();
    }

    const int workerNum = 2;
    std::shared_ptr<StreamClient> client0_;
    std::shared_ptr<StreamClient> client1_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(MemCtrlBoundaryCaseTest, RepeatRecv)
{
    std::string stream1("testRepeatRecv");
    ThreadPool pool(2);
    std::vector<std::future<Status>> futs;

    std::promise<void> topoPromise;
    std::shared_future<void> sFut = topoPromise.get_future();

    std::promise<uint64_t> promise;
    std::shared_future<uint64_t> eleNumFut = promise.get_future();

    futs.emplace_back(pool.Submit([this, &promise, &stream1, &sFut]() {
        const uint64_t eleSz0 = 16 * KB - ADDITIONAL_INFO_SZ;
        const uint64_t eleNum0 = 31;
        LOG(INFO) << FormatString("Round0, Element size: %zu, Element number: %zu", eleSz0, eleNum0);

        const uint64_t eleSz1 = 16 * KB - ADDITIONAL_INFO_SZ;
        const uint64_t eleNum1 = 31;
        LOG(INFO) << FormatString("Round1, Element size: %zu, Element number: %zu", eleSz1, eleNum1);

        auto totalEleNum = eleNum0 + eleNum1;

        // Create producer0 with PAGE_SIZE = 512KB on worker0
        ProducerConf producerConf = {
            .delayFlushTime = -1, .pageSize = BIG_PAGE_SIZE, .maxStreamSize = SMALL_SHM_SIZE_MB * MB
        };
        std::shared_ptr<Producer> producer0;
        RETURN_IF_NOT_OK(client0_->CreateProducer(stream1, producer0, producerConf));
        // Create producer1 with PAGE_SIZE = 512KB on worker0
        std::shared_ptr<Producer> producer1;
        RETURN_IF_NOT_OK(client0_->CreateProducer(stream1, producer1, producerConf));

        sFut.get();  // After consumer1 subscribed, producer0 and producer1 can send data

        // Using producer0 and producer1 to send 62 * 16KB = 992KB (Each 496KB) data to worker1
        auto producer0Id = "producer0";
        auto producer1Id = "producer1";
        RETURN_IF_NOT_OK(ProduceFixedSzData(producer0, producer0Id, eleSz0, eleNum0));
        RETURN_IF_NOT_OK(ProduceFixedSzData(producer1, producer1Id, eleSz1, eleNum1));

        promise.set_value(totalEleNum);  // After sent all (eleSz0 + eleSz1) data, consumer can receive
        RETURN_IF_NOT_OK(producer0->Close());
        RETURN_IF_NOT_OK(producer1->Close());
        return Status::OK();
    }));
    futs.emplace_back(pool.Submit([this, &stream1, &eleNumFut, &topoPromise]() {
        std::shared_ptr<Consumer> consumer1;
        SubscriptionConfig config1("sub1", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(client1_->Subscribe(stream1, config1, consumer1));
        std::shared_ptr<Producer> producer2;
        ProducerConf producerConf = {
            .delayFlushTime = -1, .pageSize = BIG_PAGE_SIZE, .maxStreamSize = SMALL_SHM_SIZE_MB * MB
        };
        RETURN_IF_NOT_OK(client1_->CreateProducer(stream1, producer2, producerConf));
        int localEleNum = 1;
        auto producer2Id = "producer2";
        RETURN_IF_NOT_OK(ProduceFixedSzData(producer2, producer2Id, (1 * KB / 8) - ADDITIONAL_INFO_SZ, localEleNum));
        RETURN_IF_NOT_OK(producer2->Close());
        topoPromise.set_value();  // Notify another thread consumer is set up

        auto remoteEleNum = eleNumFut.get();
        LOG(INFO) << FormatString("Remote element number:%zu", remoteEleNum);
        auto totalEleNum = localEleNum + remoteEleNum;

        std::vector<Element> outElements;
        thread_local auto timer = Timer();
        uint64_t timeOut = 1000;
        uint64_t lastRecvCursor = 0;
        while (timer.ElapsedMilliSecond() <= timeOut) {
            std::vector<Element> output;
            consumer1->Receive(totalEleNum, 100, output);  // Receive 63 element without wait one time
            if (!output.empty()) {
                outElements.insert(outElements.end(), output.begin(), output.end());
                lastRecvCursor += output.size();
                LOG(INFO) << FormatString("consumer1 received %zu elements in total, hence we ack to cursor:%zu",
                                          outElements.size(), lastRecvCursor);
                RETURN_IF_NOT_OK(consumer1->Ack(lastRecvCursor));  // Release shm on worker1
            }
        }
        CHECK_FAIL_RETURN_STATUS(
            outElements.size() == totalEleNum, K_INVALID,
            FormatString("Out element1 size %zu not equal to expectedNum: %zu.", outElements.size(), totalEleNum));

        RETURN_IF_NOT_OK(consumer1->Close());
        return Status::OK();
    }));
    for (auto &fut : futs) {
        ASSERT_EQ(fut.get(), Status::OK());
    }
    LOG(INFO) << "Success";
}
}  // namespace st
}  // namespace datasystem

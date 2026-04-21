/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: UcpWorkerPool test.
 */

#include <chrono>
#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include "common/rdma/create_ucp_context.h"
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/prepare_local_server.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"
#define private public
#include "datasystem/common/rdma/ucp_worker_pool.h"
#undef private
#include "datasystem/utils/status.h"

namespace datasystem {
namespace ut {
class UcpWorkerPoolTest : public ::testing::Test {
protected:
    size_t workerN_ = 5;
    std::unique_ptr<UcpWorkerPool> workerPool_;
    std::string message_ = "Das ist gut";

    std::unique_ptr<MimicRemoteServer> remoteServer_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;
    std::unique_ptr<PrepareLocalServer> localBuffer_;
    ucp_context_h context_;

    static constexpr int rdmaWaitTimeMs_ = 1500;

    void SetUp() override
    {
        bool dlopenInitResult = datasystem::ucp_dlopen::Init();
        EXPECT_EQ(dlopenInitResult, true);
        cUcpContext_ = std::make_unique<CreateUcpContext>();
        context_ = cUcpContext_->GetContext();

        remoteServer_ = std::make_unique<MimicRemoteServer>(context_);
        remoteServer_->InitUcpWorker();
        remoteServer_->InitUcpSegment();

        localBuffer_ = std::make_unique<PrepareLocalServer>(context_, message_);

        workerPool_ = std::make_unique<UcpWorkerPool>(context_, workerN_);
        Status status = workerPool_->Init();
        ASSERT_EQ(status, Status::OK());
    }

    void TearDown() override
    {
        remoteServer_.reset();
        localBuffer_.reset();
        workerPool_.reset();
        cUcpContext_.reset();
    }
};

TEST_F(UcpWorkerPoolTest, TestInitialization)
{
    // pool map should contain 5 elements
    EXPECT_EQ(workerPool_->localWorkerPool_.size(), workerN_);

    // recv map should be empty
    EXPECT_TRUE(workerPool_->localWorkerRecvMap_.empty());

    // miscellaneous
    EXPECT_EQ(workerPool_->workerN_, workerN_);
    EXPECT_EQ(workerPool_->roundRobin_.load(), 0);
}

TEST_F(UcpWorkerPoolTest, TestGetOrSelRecvWorker)
{
    // keep record of the round robin number
    auto initRR = workerPool_->roundRobin_.load();

    // keep record of the initial size of the map
    auto initMapSize = workerPool_->localWorkerRecvMap_.size();

    // insert one dummy IP
    auto chosen1 = workerPool_->GetOrSelRecvWorkerAddr("aaa");
    initRR += 1;
    initMapSize += 1;

    // map should now be populated, round robin value += 1
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initMapSize);
    EXPECT_NE(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());

    // do it again and make sure not the same worker is assigned to the task
    auto chosen2 = workerPool_->GetOrSelRecvWorkerAddr("bbb");
    initRR += 1;
    initMapSize += 1;

    EXPECT_NE(workerPool_->localWorkerRecvMap_.at("aaa"), workerPool_->localWorkerRecvMap_.at("bbb"));
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_NE(chosen1, chosen2);

    // repeat a key
    auto chosen3 = workerPool_->GetOrSelRecvWorkerAddr("aaa");
    EXPECT_EQ(chosen1, chosen3);
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initMapSize);

    // check a non-existing key
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.find("blah"), workerPool_->localWorkerRecvMap_.end());
}

TEST_F(UcpWorkerPoolTest, TestGetOrSelRecvWorkerAfterSendRoundRobinWrap)
{
    const auto initRR = workerPool_->roundRobin_.load();
    for (size_t i = 0; i < workerN_ + 2; ++i) {
        ASSERT_NE(workerPool_->GetOrSelSendWorker("send-before-recv", i), nullptr);
    }

    auto chosen = workerPool_->GetOrSelRecvWorkerAddr("recv-after-send-wrap");
    EXPECT_FALSE(chosen.empty());
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR + workerN_ + 3);
    EXPECT_EQ(workerPool_->localWorkerPool_.size(), workerN_);
    EXPECT_NE(workerPool_->localWorkerRecvMap_.find("recv-after-send-wrap"), workerPool_->localWorkerRecvMap_.end());
}

TEST_F(UcpWorkerPoolTest, TestGetOrSelSendWorker)
{
    // keep record of the round robin number
    auto initRR = workerPool_->roundRobin_.load();

    // send worker should be selected in round-robin even for the same IP
    auto chosen1 = workerPool_->GetOrSelSendWorker("aaa", 0);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);

    auto chosen2 = workerPool_->GetOrSelSendWorker("bbb", 1);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_NE(chosen1, chosen2);

    // repeat a key and verify the selection still rotates
    auto chosen3 = workerPool_->GetOrSelSendWorker("aaa", 2);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_NE(chosen1, chosen3);
    EXPECT_NE(chosen2, chosen3);
}

TEST_F(UcpWorkerPoolTest, TestWriteOnce)
{
    Status status;
    auto initRR = workerPool_->roundRobin_.load();

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(rdmaWaitTimeMs_));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
}

TEST_F(UcpWorkerPoolTest, TestWriteTwice)
{
    Status status;
    auto initRR = workerPool_->roundRobin_.load();

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(rdmaWaitTimeMs_));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);

    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(rdmaWaitTimeMs_));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
}

TEST_F(UcpWorkerPoolTest, TestRmByIp)
{
    // initial values
    auto initRR = workerPool_->roundRobin_.load();
    auto initRecvMapSize = workerPool_->localWorkerRecvMap_.size();

    // insert values to maps insert aaa as a put action, insert bbb naively
    workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                       remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    initRR += 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(rdmaWaitTimeMs_));

    workerPool_->GetOrSelSendWorker("bbb", 1);
    initRR += 1;
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);

    workerPool_->GetOrSelRecvWorkerAddr("aaa");
    initRR += 1;
    initRecvMapSize += 1;
    workerPool_->GetOrSelRecvWorkerAddr("ccc");
    initRR += 1;
    initRecvMapSize += 1;

    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);
    EXPECT_EQ(workerPool_->roundRobin_.load(), initRR);
    EXPECT_NE(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());

    EXPECT_EQ(workerPool_->RemoveByIp(std::string("aaa")), Status::OK());
    initRecvMapSize -= 1;

    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());

    EXPECT_EQ(workerPool_->RemoveByIp(std::string("aaa")), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);

    EXPECT_EQ(workerPool_->RemoveByIp(std::string("bbb")), Status::OK());

    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);

    EXPECT_EQ(workerPool_->RemoveByIp(std::string("ccc")), Status::OK());
    initRecvMapSize -= 1;

    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);
}

TEST_F(UcpWorkerPoolTest, TestRemoveByIpRemovesEndpointFromAllWorkers)
{
    const std::string ipAddr = "remove-all-workers";
    for (auto &[workerId, worker] : workerPool_->localWorkerPool_) {
        (void)workerId;
        ASSERT_NE(worker->GetOrCreateEndpoint(ipAddr, remoteServer_->GetWorkerAddr()), nullptr);
        ASSERT_NE(worker->remoteEndpointMap_.find(ipAddr), worker->remoteEndpointMap_.end());
    }

    workerPool_->GetOrSelRecvWorkerAddr(ipAddr);
    ASSERT_NE(workerPool_->localWorkerRecvMap_.find(ipAddr), workerPool_->localWorkerRecvMap_.end());

    EXPECT_EQ(workerPool_->RemoveByIp(ipAddr), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.find(ipAddr), workerPool_->localWorkerRecvMap_.end());
    for (auto &[workerId, worker] : workerPool_->localWorkerPool_) {
        (void)workerId;
        EXPECT_EQ(worker->remoteEndpointMap_.find(ipAddr), worker->remoteEndpointMap_.end());
    }
}

TEST_F(UcpWorkerPoolTest, TestClean)
{
    workerPool_->Clean();

    EXPECT_TRUE(workerPool_->localWorkerRecvMap_.empty());
    EXPECT_TRUE(workerPool_->localWorkerPool_.empty());
}

}  // namespace ut
}  // namespace datasystem

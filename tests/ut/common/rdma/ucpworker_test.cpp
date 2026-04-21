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
 * Description: UcpWorker test.
 */

#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>
#include <type_traits>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include "common/rdma/create_ucp_context.h"
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/prepare_local_server.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/rdma/ucp_endpoint.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"
#define private public
#include "datasystem/common/rdma/ucp_worker.h"
#undef private

namespace datasystem {
namespace ut {

class UcpWorkerTest : public ::testing::Test {
protected:
    std::shared_ptr<UcpWorker> ucpWorker_;
    std::string message_ = "Das ist gut";

    std::unique_ptr<MimicRemoteServer> remoteServer_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;
    std::unique_ptr<PrepareLocalServer> localBuffer_;

    ucp_context_h context_;

    static constexpr int RDMA_WAIT_TIME_MS = 1000;

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

        ucpWorker_ = std::make_shared<UcpWorker>(context_, 0);
        EXPECT_EQ(ucpWorker_->submitThread_, nullptr);
        Status status = ucpWorker_->Init();
        ASSERT_EQ(status, Status::OK());
        EXPECT_TRUE(ucpWorker_->submitThread_->joinable());
    }

    void TearDown() override
    {
        auto start = std::chrono::steady_clock::now();
        remoteServer_.reset();
        localBuffer_.reset();
        ucpWorker_.reset();
        cUcpContext_.reset();
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        LOG(INFO) << "Teardown took " << duration.count() << "ms";
    }
};

TEST_F(UcpWorkerTest, TestInitialization)
{
    EXPECT_EQ(ucpWorker_->workerId_, 0);
    EXPECT_EQ(ucpWorker_->errorMsgHead_, "[UcpWorker 0]");

    auto res1 = ucpWorker_->GetLocalWorkerAddr();
    auto res2 = ucpWorker_->GetLocalWorkerAddr();

    EXPECT_EQ(ucpWorker_->localWorkerAddrStr_, res1);
    EXPECT_EQ(res1, res2);
    EXPECT_NE(std::addressof(res1), std::addressof(res2));

    EXPECT_TRUE(ucpWorker_->submitRunning_.load());

    EXPECT_TRUE(ucpWorker_->remoteEndpointMap_.empty());
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.size(), 0);
}

TEST_F(UcpWorkerTest, TestExplicitEpCreation)
{
    std::unique_ptr<UcpEndpoint> ucpEp_ =
        std::make_unique<UcpEndpoint>(ucpWorker_->worker_, remoteServer_->GetWorkerAddr());
    Status status = ucpEp_->Init();
    ASSERT_EQ(status, Status::OK());
}

TEST_F(UcpWorkerTest, TestGetOrCreateEndpoint)
{
    EXPECT_NE(ucpWorker_->worker_, nullptr);
    auto start = std::chrono::steady_clock::now();
    auto res = ucpWorker_->GetOrCreateEndpoint("aaa", remoteServer_->GetWorkerAddr());
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "GetOrCreateEndpoint took " << duration.count() << "ms";
    EXPECT_NE(res, nullptr);
    EXPECT_FALSE(ucpWorker_->remoteEndpointMap_.empty());
}

TEST_F(UcpWorkerTest, TestWriteOnce)
{
    Status status;

    size_t initial_size = ucpWorker_->remoteEndpointMap_.size();
    auto start = std::chrono::steady_clock::now();
    status = ucpWorker_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                               remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "Write took " << duration.count() << "ms";

    std::this_thread::sleep_for(std::chrono::milliseconds(RDMA_WAIT_TIME_MS));
    LOG(INFO) << "Remote received " + remoteServer_->ReadBuffer(message_.size());
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    EXPECT_EQ(status, Status::OK());
    // no matter if the put action is successful, an entry should've been added to the map with key = "aaa"
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.size(), initial_size + 1);
    EXPECT_NE(ucpWorker_->remoteEndpointMap_.find("aaa"), ucpWorker_->remoteEndpointMap_.end());
}

TEST_F(UcpWorkerTest, TestWriteNotifiesEventThroughSubmitLoop)
{
    auto event = std::make_shared<Event>(10);
    Status status =
        ucpWorker_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                          remoteServer_->GetWorkerAddr(), "submit-loop-write", localBuffer_->Address(),
                          localBuffer_->Size(), event->GetRequestId(), event);
    ASSERT_EQ(status, Status::OK());

    Status waitStatus = event->WaitFor(std::chrono::milliseconds(RDMA_WAIT_TIME_MS));
    ASSERT_TRUE(waitStatus.IsOk()) << waitStatus.ToString();
    EXPECT_FALSE(event->IsFailed());
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
}

TEST_F(UcpWorkerTest, TestWriteNNotifiesEventThroughSubmitLoop)
{
    std::vector<IovSegment> segments{ { localBuffer_->Address(), localBuffer_->Size() } };
    auto event = std::make_shared<Event>(11);
    Status status =
        ucpWorker_->WriteN(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "submit-loop-writen", segments, event->GetRequestId(),
                           event);
    ASSERT_EQ(status, Status::OK());

    Status waitStatus = event->WaitFor(std::chrono::milliseconds(RDMA_WAIT_TIME_MS));
    ASSERT_TRUE(waitStatus.IsOk()) << waitStatus.ToString();
    EXPECT_FALSE(event->IsFailed());
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
}

TEST_F(UcpWorkerTest, TestWriteFailsAfterSubmitThreadStopped)
{
    ucpWorker_->StopSubmitThread();
    auto event = std::make_shared<Event>(12);
    Status status =
        ucpWorker_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                          remoteServer_->GetWorkerAddr(), "submit-loop-stopped", localBuffer_->Address(),
                          localBuffer_->Size(), event->GetRequestId(), event);
    EXPECT_TRUE(status.IsError());
    EXPECT_EQ(status.GetCode(), K_RDMA_ERROR);
}

TEST_F(UcpWorkerTest, TestRmEmptyMap)
{
    size_t initial_size = ucpWorker_->remoteEndpointMap_.size();

    Status status = ucpWorker_->RemoveEndpointByIp(std::string("aaa"));
    // since we have added "aaa" to map, we should expect the removal to be successful.
    EXPECT_EQ(status, Status::OK());
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.size(), initial_size);
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.find("aaa"), ucpWorker_->remoteEndpointMap_.end());
}

TEST_F(UcpWorkerTest, TestWriteAndRm)
{
    size_t initial_size = ucpWorker_->remoteEndpointMap_.size();

    Status status;
    auto start = std::chrono::steady_clock::now();
    status = ucpWorker_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                               remoteServer_->GetWorkerAddr(), "bbb", localBuffer_->Address(), localBuffer_->Size(), 1);
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "Write took " << duration.count() << "ms";
    // simply testing if blocking returns any error message
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(RDMA_WAIT_TIME_MS));
    LOG(INFO) << "Remote received " + remoteServer_->ReadBuffer(message_.size());
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.size(), initial_size + 1);
    EXPECT_NE(ucpWorker_->remoteEndpointMap_.find("bbb"), ucpWorker_->remoteEndpointMap_.end());

    start = std::chrono::steady_clock::now();
    status = ucpWorker_->RemoveEndpointByIp(std::string("bbb"));
    end = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "RemoveEndpointByIp took " << duration.count() << "ms";
    // since we have added "aaa" to map, we should expect the removal to be successful.
    EXPECT_EQ(status, Status::OK());
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.size(), initial_size);
    EXPECT_EQ(ucpWorker_->remoteEndpointMap_.find("bbb"), ucpWorker_->remoteEndpointMap_.end());
}

TEST_F(UcpWorkerTest, TestClean)
{
    ucpWorker_->Clean();

    EXPECT_EQ(ucpWorker_->localWorkerAddr_, nullptr);
    EXPECT_EQ(ucpWorker_->worker_, nullptr);
    EXPECT_TRUE(ucpWorker_->remoteEndpointMap_.empty());
}

TEST_F(UcpWorkerTest, TestStopSubmitThread)
{
    ucpWorker_->StopSubmitThread();
    EXPECT_EQ(ucpWorker_->submitThread_, nullptr);
}
}  // namespace ut
}  // namespace datasystem

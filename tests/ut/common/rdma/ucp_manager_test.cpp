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
 * Description: Mimic a remote server, prepare a local buffer and provide info
 * needed as if sent by a remote server. Tool for Ucp tests.
 */
#include "datasystem/common/rdma/ucp_worker_pool.h"
#include "datasystem/protos/utils.pb.h"
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/prepare_local_server.h"
#include "datasystem/common/util/thread_local.h"

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#define private public
#include "datasystem/common/rdma/ucp_manager.h"
#undef private

constexpr uint64_t PORT = 8080;
constexpr uint64_t TIME = 1;
namespace datasystem {
namespace ut {
class UcpManagerTestFriend {
public:
    static void Reset(UcpManager &manager)
    {
        manager.workerPool_.reset();
        manager.localSegmentMap_.reset();
        if (manager.ucpContext_) {
            manager.UcpDeleteContext();
        }
        manager.eventMap_.reset();
    }
};
class UcpManagerTest : public ::testing::Test {
protected:
    void SetUp() override

    {
        manager_ = &UcpManager::Instance();
        ASSERT_TRUE(manager_->Init().IsOk());
        localBuffer_ = std::make_unique<PrepareLocalServer>(manager_->ucpContext_, message_);
    }
    void TearDown() override
    {
        localBuffer_.reset();
        UcpManagerTestFriend::Reset(*manager_);
        manager_ = nullptr;
    }
    UcpManager *manager_{ nullptr };
    std::unique_ptr<PrepareLocalServer> localBuffer_;
    std::string message_ = "Das ist gut";
};

TEST_F(UcpManagerTest, SingletonPattern)
{
    UcpManager &instance1 = UcpManager::Instance();
    UcpManager &instance2 = UcpManager::Instance();
    EXPECT_EQ(&instance1, &instance2);
}

TEST_F(UcpManagerTest, GetRecvWorkerAddr)
{
    std::string ipAddr1 = "127.0.0.1:8080";
    std::string ipAddr2 = "192.168.1.12:8080";
    std::string workerAddr1 = manager_->GetRecvWorkerAddress(ipAddr1);
    std::string workerAddr2 = manager_->GetRecvWorkerAddress(ipAddr1);
    std::string workerAddr3 = manager_->GetRecvWorkerAddress(ipAddr2);
    EXPECT_FALSE(workerAddr1.empty());
    EXPECT_EQ(workerAddr1, workerAddr2);
    EXPECT_NE(workerAddr1, workerAddr3);
}

TEST_F(UcpManagerTest, SegmentRegistration)
{
    void *buffer = malloc(4096);
    ASSERT_NE(buffer, nullptr);
    const uint64_t segAddress = reinterpret_cast<uint64_t>(buffer);
    const uint64_t segSize = 4096;
    EXPECT_EQ(manager_->RegisterSegment(segAddress, segSize), Status::OK());
    free(buffer);
}

TEST_F(UcpManagerTest, SegmentRegistrationDuplicate)
{
    void *buffer = malloc(4096);
    ASSERT_NE(buffer, nullptr);
    const uint64_t segAddress = reinterpret_cast<uint64_t>(buffer);
    const uint64_t segSize = 4096;
    EXPECT_EQ(manager_->RegisterSegment(segAddress, segSize), Status::OK());
    EXPECT_EQ(manager_->RegisterSegment(segAddress, segSize), Status::OK());
    free(buffer);
}

TEST_F(UcpManagerTest, FillUcpInfoImplementation)
{
    uint64_t dataOffset = 256;
    std::string srcIpAddr = "127.0.0.1:" + std::to_string(PORT);
    void *buffer = malloc(4096);
    ASSERT_NE(buffer, nullptr);
    const uint64_t segAddress = reinterpret_cast<uint64_t>(buffer);
    const uint64_t segSize = 4096;
    EXPECT_EQ(manager_->RegisterSegment(segAddress, segSize), Status::OK());
    UcpRemoteInfoPb pb;
    EXPECT_EQ(manager_->FillUcpInfoImpl(segAddress, dataOffset, srcIpAddr, pb), Status::OK());
    EXPECT_EQ(pb.remote_buf(), segAddress + dataOffset);
    UcpSegmentMap::ConstAccessor accessor;
    ASSERT_TRUE(manager_->localSegmentMap_->Find(accessor, segAddress));
    std::string expectedRkey = accessor.entry->data.GetPackedRkey();
    EXPECT_EQ(pb.rkey(), expectedRkey);
    std::string expectedWorkerAddr = manager_->GetRecvWorkerAddress(srcIpAddr);
    EXPECT_EQ(pb.remote_worker_addr(), expectedWorkerAddr);
    free(buffer);
}

TEST_F(UcpManagerTest, ImportSegAndUcpPutPayloadBlocking)
{
    std::unique_ptr<MimicRemoteServer> remoteServer_;
    remoteServer_ = std::make_unique<MimicRemoteServer>(manager_->ucpContext_);
    remoteServer_->InitUcpWorker();
    remoteServer_->InitUcpSegment();  
    uint64_t localObjectAddress = localBuffer_->Address();
    UcpRemoteInfoPb rdmaInfo;
    auto *remoteAddr = rdmaInfo.mutable_remote_ip_addr();
    remoteAddr->set_host("127.0.0.1");
    remoteAddr->set_port(PORT);
    rdmaInfo.set_remote_worker_addr(remoteServer_->GetWorkerAddr());
    rdmaInfo.set_rkey(remoteServer_->GetPackedRkey());
    rdmaInfo.set_remote_buf((uintptr_t)remoteServer_->GetLocalSegAddr());
    uint64_t readOffset = 0;
    uint64_t readSize = localBuffer_->Size();
    uint64_t metaDataSize = 0;
    bool blocking = true;
    std::vector<uint64_t> keys;
    Status status =
        manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize, metaDataSize, blocking, keys);
    EXPECT_EQ(status, Status::OK());
    EXPECT_TRUE(keys.empty());
    remoteServer_.reset();
}

TEST_F(UcpManagerTest, ImportSegAndUcpPutPayloadNonBlocking)
{
    std::unique_ptr<MimicRemoteServer> remoteServer_;
    remoteServer_ = std::make_unique<MimicRemoteServer>(manager_->ucpContext_);
    remoteServer_->InitUcpWorker();
    remoteServer_->InitUcpSegment(); 
    uint64_t localObjectAddress = localBuffer_->Address();
    UcpRemoteInfoPb rdmaInfo;
    auto *remoteAddr = rdmaInfo.mutable_remote_ip_addr();
    remoteAddr->set_host("127.0.0.1");
    remoteAddr->set_port(PORT);
    rdmaInfo.set_remote_worker_addr(remoteServer_->GetWorkerAddr());
    rdmaInfo.set_rkey(remoteServer_->GetPackedRkey());
    rdmaInfo.set_remote_buf((uintptr_t)remoteServer_->GetLocalSegAddr());
    uint64_t readOffset = 0;
    uint64_t readSize = localBuffer_->Size();
    uint64_t metaDataSize = 0;
    bool blocking = false;
    std::vector<uint64_t> keys;
    Status status =
        manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize, metaDataSize, blocking, keys);
    EXPECT_EQ(status, Status::OK());
    EXPECT_EQ(keys.size(), 1);
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(1)) {
        ds_ucp_worker_progress(remoteServer_->GetWorker());
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    for (uint64_t key : keys) {
        std::shared_ptr<Event> event;
        EXPECT_EQ(manager_->GetEvent(key, event), Status::OK());
        EXPECT_NE(event, nullptr);
        // Wait for the event to be notified (callback notifies directly now)
        Status waitStatus = event->WaitFor(std::chrono::milliseconds(1000));
        EXPECT_TRUE(waitStatus.IsOk());
    }
    remoteServer_.reset();
}

TEST_F(UcpManagerTest, InsertEventsNotifiesDirectly)
{
    uint64_t requestId = 123;
    std::shared_ptr<Event> event;
    EXPECT_EQ(manager_->CreateEvent(requestId, event), Status::OK());
    
    // InsertSuccessfulEvent should notify the event directly
    manager_->InsertSuccessfulEvent(requestId);
    // Event should be ready now (notified)
    Status waitStatus = event->WaitFor(std::chrono::milliseconds(0));
    EXPECT_TRUE(waitStatus.IsOk());
    
    // Test failed event
    uint64_t failedRequestId = 456;
    std::shared_ptr<Event> failedEvent;
    EXPECT_EQ(manager_->CreateEvent(failedRequestId, failedEvent), Status::OK());
    manager_->InsertFailedEvent(failedRequestId);
    waitStatus = failedEvent->WaitFor(std::chrono::milliseconds(0));
    EXPECT_TRUE(waitStatus.IsOk());
    EXPECT_TRUE(failedEvent->IsFailed());
}

TEST_F(UcpManagerTest, EventManagement)
{
    std::shared_ptr<Event> event;
    const uint64_t requestId = 12345;
    EXPECT_EQ(manager_->CreateEvent(requestId, event), Status::OK());
    EXPECT_NE(event, nullptr);
    std::shared_ptr<Event> retrievedEvent;
    EXPECT_TRUE(manager_->GetEvent(requestId, retrievedEvent).IsOk());
    EXPECT_EQ(event, retrievedEvent);
    manager_->DeleteEvent(requestId);
    Status status = manager_->GetEvent(requestId, retrievedEvent);
    EXPECT_FALSE(status.IsOk());
}

TEST_F(UcpManagerTest, WaitToFinishTimeout)
{
    std::shared_ptr<Event> event;
    const uint64_t requestId = 12345;
    manager_->CreateEvent(requestId, event);
    Status status = manager_->WaitToFinish(requestId, 10);
    EXPECT_FALSE(status.IsOk());
}

TEST_F(UcpManagerTest, UseAddrRemoveEndpoint)
{
    std::unique_ptr<MimicRemoteServer> remoteServer_;
    remoteServer_ = std::make_unique<MimicRemoteServer>(manager_->ucpContext_);
    remoteServer_->InitUcpWorker();
    remoteServer_->InitUcpSegment();
    uint64_t localObjectAddress = localBuffer_->Address();
    UcpRemoteInfoPb rdmaInfo;
    auto *remoteAddr = rdmaInfo.mutable_remote_ip_addr();
    remoteAddr->set_host("127.0.0.1");
    remoteAddr->set_port(PORT);
    rdmaInfo.set_remote_worker_addr(remoteServer_->GetWorkerAddr());
    rdmaInfo.set_rkey(remoteServer_->GetPackedRkey());
    rdmaInfo.set_remote_buf((uintptr_t)remoteServer_->GetLocalSegAddr());
    uint64_t readOffset = 0;
    uint64_t readSize = localBuffer_->Size();
    uint64_t metaDataSize = 0;
    bool blocking = true;
    std::vector<uint64_t> keys;
    Status status =
        manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize, metaDataSize, blocking, keys);
    EXPECT_EQ(status, Status::OK());

    manager_->GetRecvWorkerAddress("127.0.0.1:8080");
    HostPort remoteAddress("127.0.0.1", PORT);
    Status removeStatus = manager_->RemoveEndpoint(remoteAddress);
    EXPECT_EQ(removeStatus, Status::OK());
    remoteServer_.reset();
}

TEST_F(UcpManagerTest, ConcurrentUcpPutPayload)
{
    const int numThreads = 4;
    std::vector<std::thread> threads;
    std::atomic<int> failureCount{ 0 };
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, &failureCount]() {
            try {
                std::unique_ptr<MimicRemoteServer> remoteServer_;
                remoteServer_ = std::make_unique<MimicRemoteServer>(manager_->ucpContext_);
                remoteServer_->InitUcpWorker();
                remoteServer_->InitUcpSegment(); 
                uint64_t localObjectAddress = localBuffer_->Address();
                UcpRemoteInfoPb rdmaInfo;
                auto *remoteAddr = rdmaInfo.mutable_remote_ip_addr();
                remoteAddr->set_host("127.0.0.1");
                remoteAddr->set_port(PORT);
                rdmaInfo.set_remote_worker_addr(remoteServer_->GetWorkerAddr());
                rdmaInfo.set_rkey(remoteServer_->GetPackedRkey());
                rdmaInfo.set_remote_buf((uintptr_t)remoteServer_->GetLocalSegAddr());
                uint64_t readOffset = 0;
                uint64_t readSize = localBuffer_->Size();
                uint64_t metaDataSize = 0;
                bool blocking = false;
                std::vector<uint64_t> keys;
                Status status = manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize,
                                                        metaDataSize, blocking, keys);
                EXPECT_EQ(status, Status::OK());
                EXPECT_EQ(keys.size(), 1);
                auto start = std::chrono::steady_clock::now();
                while (std::chrono::steady_clock::now() - start < std::chrono::seconds(1)) {
                    ds_ucp_worker_progress(remoteServer_->GetWorker());
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
                for (uint64_t key : keys) {
                    std::shared_ptr<Event> event;
                    EXPECT_EQ(manager_->GetEvent(key, event), Status::OK());
                    EXPECT_NE(event, nullptr);
                    // Wait for the event to be notified (callback notifies directly now)
                    Status waitStatus = event->WaitFor(std::chrono::milliseconds(2000));
                    EXPECT_TRUE(waitStatus.IsOk());
                }
                remoteServer_.reset();
            } catch (const std::exception &e) {
                ADD_FAILURE() << "Thread " << i << " threw exception: " << e.what();
                failureCount++;
            } catch (...) {
                ADD_FAILURE() << "Thread " << i << " threw unknown exception";
                failureCount++;
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    EXPECT_EQ(failureCount.load(), 0) << "Some threads failed during concurrent UcpPutPayload";
}

}  // namespace ut
}  // namespace datasystem

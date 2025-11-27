#include "datasystem/common/rdma/ucp_manager.h"
#include "datasystem/common/rdma/ucp_worker_pool.h"
#include "datasystem/protos/utils.pb.h"

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

namespace datasystem {
namespace ut {
class UcpManagerTestFriend {
public:
    static void Reset(UcpManager &m)
    {
        m.Stop();
        m.workerPool_.reset();
        m.localSegmentMap_.reset();
        if (m.ucpContext_) {
            m.UcpDeleteContext();
        }
        m.eventMap_.reset();
    }
};
class UcpManagerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        manager_ = &UcpManager::Instance();
        ASSERT_TRUE(manager_->Init().IsOk());
    }
    void TearDown() override
    {
        UcpManagerTestFriend::Reset(*manager_);
        manager_ = nullptr;
    }
    UcpManager *manager_{ nullptr };
};

TEST_F(UcpManagerTest, SingletonPattern)
{
    UcpManager &instance1 = UcpManager::Instance();
    UcpManager &instance2 = UcpManager::Instance();
    EXPECT_EQ(&instance1, &instance2);
}

TEST_F(UcpManagerTest, GetRecvWorkerAddr)
{
    std::string ip1 = "192.168.1.1:8080";
    std::string ip2 = "192.168.1.12:8080";
    std::string addr1 = manager_->GetRecvWorkerAddress(ip1);
    std::string addr2 = manager_->GetRecvWorkerAddress(ip1);
    std::string addr3 = manager_->GetRecvWorkerAddress(ip2);
    EXPECT_FALSE(addr1.empty());
    EXPECT_EQ(addr1, addr2);
    EXPECT_NE(addr1, addr3);
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

// TEST_F(UcpManagerTest, FillUcpInfoimpl)
// {
//     uint64_t dataOffset = 256;
//     std::string srcIpAddr = "192.168.1.1";
//     void *buffer = malloc(4096);
//     ASSERT_NE(buffer, nullptr);
//     const uint64_t segAddress = reinterpret_cast<uint64_t>(buffer);
//     const uint64_t segSize = 4096;
//     EXPECT_EQ(manager_->RegisterSegment(segAddress, segSize), Status::OK());
//     UcpRemoteInfoPb pb;
//     EXPECT_EQ(manager_->FillUcpInfoImpl(segAddress, dataOffset, srcIpAddr, pb), Status::OK());
//     EXPECT_EQ(pb.remote_buf(), segAddress + dataOffset);
//     UcpSegmentMap::ConstAccessor accessor;
//     ASSERT_TRUE(manager_->localSegmentMap_->Find(accessor, segAddress));
//     std::string expectedRkey = accessor.entry->data.GetPackedRkey();
//     EXPECT_EQ(pb.rkey(), expectedRkey);
//     std::string expectedWorkerAddr = manager_->GetRecvWorkerAddress(srcIpAddr);
//     EXPECT_EQ(pb.remote_worker_addr(), expectedWorkerAddr);
//     free(buffer);
// }

// TEST_F(UcpManagerTest, ImportSegAndUcpPutPayloadBlocking)
// {
//     UcpRemoteInfoPb rdmaInfo;
//     rdmaInfo.set_remote_worker_addr(std::string(16, 0));
//     rdmaInfo.set_remote_buf(0x2000);
//     rdmaInfo.set_rkey(std::string(32, 'k'));
//     auto *remote_addr = rdmaInfo.mutable_remote_ip_addr();
//     remote_addr->set_host("127.0.0.1");
//     remote_addr->set_port(8081);
//     uint64_t localObjectAddress = 0x1000;
//     uint64_t readOffset = 0;
//     uint64_t readSize = 4096;
//     uint64_t metaDataSize = 0x20;
//     bool blocking = true;
//     std::vector<uint64_t> keys;
//     Status status =
//         manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize, metaDataSize, blocking, keys);
//     EXPECT_EQ(status, Status::OK());
//     EXPECT_EQ(keys.empty(), true);
// }

// TEST_F(UcpManagerTest, ImportSegAndUcpPutPayloadNonBlocking)
// {
//     UcpRemoteInfoPb rdmaInfo;
//     std::string ip = "192.168.1.1:8080";
//     std::string remoteWorkerAddr = manager_->GetRecvWorkerAddress(ip);
//     rdmaInfo.set_remote_worker_addr(remoteWorkerAddr);
//     rdmaInfo.set_remote_buf(0x2000);
//     rdmaInfo.set_rkey(std::string(32, 'k'));
//     auto *remote_addr = rdmaInfo.mutable_remote_ip_addr();
//     remote_addr->set_host("127.0.0.1");
//     remote_addr->set_port(8081);
//     uint64_t localObjectAddress = 0x1000;
//     uint64_t readOffset = 0;
//     uint64_t readSize = 512 * 1024 * 1024 + 100;
//     uint64_t metaDataSize = 0x20;
//     bool blocking = false;
//     std::vector<uint64_t> keys;
//     Status status =
//         manager_->UcpPutPayload(rdmaInfo, localObjectAddress, readOffset, readSize, metaDataSize, blocking, keys);
//     EXPECT_EQ(status, Status::OK());
//     EXPECT_EQ(keys.size(), 2);
//     for (auto key : keys) {
//         std::shared_ptr<Event> event;
//         EXPECT_EQ(manager_->GetEvent(key, event), Status::OK());
//         EXPECT_NE(event, nullptr);
//     }
// }

TEST_F(UcpManagerTest, InsertEventsModifiesInternalSets)
{
    uint64_t requestId = 123;
    manager_->InsertSuccessfulEvent(requestId);
    EXPECT_EQ(manager_->finishedRequests_.count(requestId), 1);
    manager_->InsertFailedEvent(requestId);
    EXPECT_EQ(manager_->failedRequests_.count(requestId), 1);
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
    Status status = manager_->WaitToFinish(requestId, 10);  // 10ms
    EXPECT_FALSE(status.IsOk());
}

TEST_F(UcpManagerTest, UseAddrRemoveEndpoint)
{
    HostPort remoteAddress("127.0.0.1", 8081);
    Status status = manager_->RemoveEndpoint(remoteAddress);
    EXPECT_FALSE(status.IsOk());
}

}  // namespace ut
}  // namespace datasystem

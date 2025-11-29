#include <gtest/gtest.h>
#include <cstring>
#include <memory>
#include <chrono>
#include <thread>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"
#include "datasystem/utils/status.h"

#define private public
#include "datasystem/common/rdma/ucp_worker_pool.h"
#undef private
#include "common/rdma/mock_ucp_manager.h"
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/create_ucp_context.h"
#include "common/rdma/prepare_local_server.h"

namespace datasystem {
namespace ut {
class UcpWorkerPoolTest : public ::testing::Test {
protected:
    size_t workerN_ = 5;
    std::unique_ptr<UcpWorkerPool> workerPool_;
    std::string message_ = "Das ist gut";

    std::unique_ptr<MimicRemoteServer> remoteServer_;
    std::unique_ptr<MockUcpManager> ucpManager_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;
    std::unique_ptr<LocalBuffer> localBuffer_;
    ucp_context_h context_;

    void SetUp() override
    {
        ucpManager_ = std::make_unique<MockUcpManager>();
        cUcpContext_ = std::make_unique<CreateUcpContext>();
        context_ = cUcpContext_->GetContext();

        remoteServer_ = std::make_unique<MimicRemoteServer>(context_);
        remoteServer_->InitUcpWorker();
        remoteServer_->InitUcpSegment();

        localBuffer_ = std::make_unique<LocalBuffer>(context_, message_);

        workerPool_ = std::make_unique<UcpWorkerPool>(context_, ucpManager_.get(), workerN_);
        Status status = workerPool_->Init();
        ASSERT_EQ(status, Status::OK());
    }

    void TearDown() override
    {
        remoteServer_.reset();
        localBuffer_.reset();
        workerPool_.reset();
        ucpManager_.reset();
        cUcpContext_.reset();
    }
};

TEST_F(UcpWorkerPoolTest, TestInitialization)
{
    // pool map should contain 5 elements
    EXPECT_EQ(workerPool_->localWorkerPool_.size(), workerN_);

    // all maps should be empty
    EXPECT_TRUE(workerPool_->localWorkerSendMap_.empty());
    EXPECT_TRUE(workerPool_->localWorkerRecvMap_.empty());

    // miscellaneous
    EXPECT_EQ(workerPool_->workerN_, workerN_);
    EXPECT_EQ(workerPool_->roundRobin_, 0);
}

TEST_F(UcpWorkerPoolTest, TestGetOrSelRecvWorker)
{
    // keep record of the round robin number
    auto initRR = workerPool_->roundRobin_.load();

    // keep record of the initial size of the map
    auto initMapSize = workerPool_->localWorkerRecvMap_.size();

    // insert one dummy IP
    auto chosen1 = workerPool_->GetOrSelRecvWorkerAddr("aaa");

    // map should now be populated, round robin value += 1 modulo the size of the pool
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 1) % workerPool_->localWorkerPool_.size());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initMapSize + 1);
    EXPECT_NE(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());

    // do it again and make sure not the same worker is assigned to the task
    auto chosen2 = workerPool_->GetOrSelRecvWorkerAddr("bbb");
    EXPECT_NE(workerPool_->localWorkerRecvMap_.at("aaa"), workerPool_->localWorkerRecvMap_.at("bbb"));
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 2) % workerPool_->localWorkerPool_.size());
    EXPECT_NE(chosen1, chosen2);

    // repeat a key
    auto chosen3 = workerPool_->GetOrSelRecvWorkerAddr("aaa");
    EXPECT_EQ(chosen1, chosen3);
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 2) % workerPool_->localWorkerPool_.size());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initMapSize + 2);

    // check a non-existing key
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.find("blah"), workerPool_->localWorkerRecvMap_.end());
}

TEST_F(UcpWorkerPoolTest, TestGetOrSelSendWorker)
{
    // keep record of the round robin number
    auto initRR = workerPool_->roundRobin_.load();

    // keep record of the initial size of the map
    auto initMapSize = workerPool_->localWorkerSendMap_.size();

    // insert one dummy IP
    auto chosen1 = workerPool_->GetOrSelSendWorker("aaa");

    // map should now be populated, round robin value += 1 modulo the size of the pool
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 1) % workerPool_->localWorkerPool_.size());
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initMapSize + 1);
    EXPECT_NE(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());

    // do it again and make sure not the same worker is assigned to the task
    auto chosen2 = workerPool_->GetOrSelSendWorker("bbb");
    EXPECT_NE(workerPool_->localWorkerSendMap_.at("aaa"), workerPool_->localWorkerSendMap_.at("bbb"));
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 2) % workerPool_->localWorkerPool_.size());
    EXPECT_NE(chosen1, chosen2);

    // repeat a key
    auto chosen3 = workerPool_->GetOrSelSendWorker("aaa");
    EXPECT_EQ(chosen1, chosen3);
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 2) % workerPool_->localWorkerPool_.size());
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initMapSize + 2);

    // check a non-existing key
    EXPECT_EQ(workerPool_->localWorkerSendMap_.find("blah"), workerPool_->localWorkerSendMap_.end());
}

TEST_F(UcpWorkerPoolTest, TestWriteOnce)
{
    Status status;
    // keep record of the round robin number
    // TODO:
    // auto initRR = workerPool_->roundRobin_.load();
    // keep record of the initial size of the map
    auto initMapSize = workerPool_->localWorkerSendMap_.size();
    auto initRR = workerPool_->roundRobin_.load();

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    // should have inserted "aaa" into send map
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initMapSize + 1);
    EXPECT_NE(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 1) % workerPool_->localWorkerPool_.size());
}

TEST_F(UcpWorkerPoolTest, TestWriteTwice)
{
    Status status;
    // keep record of the round robin number
    // TODO:
    // auto initRR = workerPool_->roundRobin_.load();
    // keep record of the initial size of the map
    auto initMapSize = workerPool_->localWorkerSendMap_.size();
    auto initRR = workerPool_->roundRobin_.load();

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);

    // should have inserted "aaa" into send map
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initMapSize + 1);
    EXPECT_NE(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 1) % workerPool_->localWorkerPool_.size());

    status =
        workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                           remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);
    EXPECT_EQ(status, Status::OK());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_EQ(remoteServer_->ReadBuffer(message_.size()), message_);
    // should reuse the existing map entry
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initMapSize + 1);
    EXPECT_NE(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 1) % workerPool_->localWorkerPool_.size());
}

TEST_F(UcpWorkerPoolTest, TestRmByIp)
{
    // initial values
    auto initRR = workerPool_->roundRobin_.load();
    auto initSendMapSize = workerPool_->localWorkerSendMap_.size();
    auto initRecvMapSize = workerPool_->localWorkerRecvMap_.size();

    // insert values to maps insert aaa as a put action, insert bbb naively
    workerPool_->Write(remoteServer_->GetPackedRkey(), (uintptr_t)remoteServer_->GetLocalSegAddr(),
                       remoteServer_->GetWorkerAddr(), "aaa", localBuffer_->Address(), localBuffer_->Size(), 0);

    workerPool_->GetOrSelSendWorker("bbb");
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initSendMapSize + 2);
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 2) % workerPool_->localWorkerPool_.size());
    EXPECT_NE(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());

    workerPool_->GetOrSelRecvWorkerAddr("aaa");
    workerPool_->GetOrSelRecvWorkerAddr("ccc");
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize + 2);
    EXPECT_EQ(workerPool_->roundRobin_, (initRR + 4) % workerPool_->localWorkerPool_.size());
    EXPECT_NE(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());

    EXPECT_EQ(workerPool_->RmByIp(std::string("aaa")), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize + 1);
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initSendMapSize + 1);
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.find("aaa"), workerPool_->localWorkerRecvMap_.end());
    EXPECT_EQ(workerPool_->localWorkerSendMap_.find("aaa"), workerPool_->localWorkerSendMap_.end());

    EXPECT_NE(workerPool_->RmByIp(std::string("aaa")), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize + 1);
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initSendMapSize + 1);

    EXPECT_NE(workerPool_->RmByIp(std::string("bbb")), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize + 1);
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initSendMapSize);

    EXPECT_NE(workerPool_->RmByIp(std::string("ccc")), Status::OK());
    EXPECT_EQ(workerPool_->localWorkerRecvMap_.size(), initRecvMapSize);
    EXPECT_EQ(workerPool_->localWorkerSendMap_.size(), initSendMapSize);
}

TEST_F(UcpWorkerPoolTest, TestClean)
{
    workerPool_->Clean();

    EXPECT_TRUE(workerPool_->localWorkerSendMap_.empty());
    EXPECT_TRUE(workerPool_->localWorkerRecvMap_.empty());
}

}  // namespace ut
}  // namespace datasystem
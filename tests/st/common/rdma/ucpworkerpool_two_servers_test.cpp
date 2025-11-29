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

/**
 * Description: UcpWorkerPool RDMA write system test. Designed to be run
 * on two servers.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdint>
#include <vector>
#include <memory>
#include <unistd.h>
#include <iomanip>

#include "arpa/inet.h"
#include "sys/socket.h"
#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/rdma/ucp_worker_pool.h"
#include "datasystem/common/rdma/ucp_segment.h"
#include "common/rdma/mock_ucp_manager.h"
#include "common/rdma/create_ucp_context.h"
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/prepare_local_server.h"

namespace datasystem {
namespace st {

class UcpWorkerPool2ServersTest : public ::testing::Test {
protected:
    size_t workerN_ = 1;
    std::string message_ = "hello dummy";
    std::string serverIP_ = "10.90.41.117";
    std::string clientIP_ = "10.90.41.116";
    std::uint64_t port_ = 1252;

    std::unique_ptr<MockUcpManager> ucpManager_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;
    std::unique_ptr<UcpWorkerPool> workerPool_;

    ucp_context_h context_;

    bool serverMode_ = false;
    bool clientMode_ = false;

    int serverSock_;
    int clientSock_;
    int serverFd_;

    void SetUp() override
    {
        ucpManager_ = std::make_unique<MockUcpManager>();
        cUcpContext_ = std::make_unique<CreateUcpContext>();
        context_ = cUcpContext_->GetContext();

        workerPool_ = std::make_unique<UcpWorkerPool>(context_, ucpManager_.get(), workerN_);
        Status status = workerPool_->Init();
        ASSERT_EQ(status, Status::OK());
    }

    void TearDown() override
    {
        if (serverMode_) {
            close(clientSock_);
            close(serverFd_);
        }

        if (clientMode_) {
            close(serverSock_);
        }

        workerPool_.reset();
        ucpManager_.reset();
        cUcpContext_.reset();
    }

    void EstablishConnectionToServer()
    {
        serverSock_ = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        inet_pton(AF_INET, serverIP_.c_str(), &addr.sin_addr);

        ASSERT_FALSE(connect(serverSock_, (sockaddr *)&addr, sizeof(addr)) < 0);
    }

    void WaitForConnectionFromClient()
    {
        serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        bind(serverFd_, (sockaddr *)&addr, sizeof(addr));
        listen(serverFd_, 1);

        clientSock_ = accept(serverFd_, nullptr, nullptr);
    }
};

TEST_F(UcpWorkerPool2ServersTest, TestRunOnServerToReceiveContent)
{
    serverMode_ = true;
    // get a worker address for receiving
    auto workerAddr = workerPool_->GetOrSelRecvWorkerAddr(clientIP_);
    ASSERT_FALSE(workerAddr.empty());

    std::unique_ptr<MimicRemoteServer> localBuffer_ = std::make_unique<MimicRemoteServer>(context_);
    localBuffer_->InitUcpSegment();

    std::unique_ptr<UcpSegment> ucpSegment_ = std::make_unique<UcpSegment>((uintptr_t)localBuffer_->GetLocalSegAddr(),
                                                                           localBuffer_->GetLocalSegSize(), context_);

    Status status = ucpSegment_->Init();
    ASSERT_EQ(status, Status::OK());

    WaitForConnectionFromClient();
    LOG(INFO) << "Client Connected to me";

    size_t len = workerAddr.size();

    // send worker address to client
    EXPECT_TRUE(write(clientSock_, &len, sizeof(len)) > 0);
    EXPECT_TRUE(write(clientSock_, workerAddr.data(), workerAddr.size()) > 0);

    // send packedRkey to client
    auto packedRkey = ucpSegment_->GetPackedRkey();
    size_t rkeySize = packedRkey.size();

    EXPECT_TRUE(write(clientSock_, &rkeySize, sizeof(rkeySize)) > 0);
    EXPECT_TRUE(write(clientSock_, packedRkey.data(), rkeySize) > 0);

    // send buffer pointer to client
    uintptr_t bufferAddr = reinterpret_cast<uintptr_t>(ucpSegment_->GetLocalSegAddr());
    EXPECT_TRUE(write(clientSock_, &bufferAddr, sizeof(bufferAddr)) > 0);

    // wait for a signal that the put has been done
    uint8_t msgPut;
    LOG(INFO) << "Waiting for signal";
    EXPECT_TRUE(read(clientSock_, &msgPut, sizeof(msgPut)) > 0);

    const char *datas = reinterpret_cast<const char *>(bufferAddr);
    std::string msgRecvd(datas, message_.size());
    LOG(INFO) << FormatString("Message Received: %s", msgRecvd);
    EXPECT_EQ(msgRecvd, message_);
}

TEST_F(UcpWorkerPool2ServersTest, TestRunOnClientToSendContent)
{
    clientMode_ = true;
    EstablishConnectionToServer();
    LOG(INFO) << "Connected to server";

    // receive server worker address
    size_t len = 0;
    EXPECT_TRUE(read(serverSock_, &len, sizeof(len)) > 0);

    std::string workerAddr;
    workerAddr.resize(len);
    EXPECT_TRUE(read(serverSock_, workerAddr.data(), len) > 0);

    // receive server packed rkey
    len = 0;
    std::string packedRkey;
    EXPECT_TRUE(read(serverSock_, &len, sizeof(len)) > 0);
    packedRkey.resize(len);
    EXPECT_TRUE(read(serverSock_, packedRkey.data(), len) > 0);

    // receive server buffer address
    uintptr_t remoteBufAddr;
    EXPECT_TRUE(read(serverSock_, &remoteBufAddr, sizeof(remoteBufAddr)) > 0);

    // execute write
    LOG(INFO) << FormatString("Message before put: %s", message_.c_str());

    std::unique_ptr<LocalBuffer> localBuffer_ = std::make_unique<LocalBuffer>(context_, message_);

    Status status = workerPool_->Write(packedRkey, remoteBufAddr, workerAddr, serverIP_.c_str(),
                                       localBuffer_->Address(), localBuffer_->Size(), 0);

    LOG(INFO) << "Asynchronous put action executed";
    EXPECT_EQ(status, Status::OK());

    // send put finished message after wait a bit
    sleep(1);
    bool putDone = true;
    EXPECT_TRUE(write(serverSock_, &putDone, sizeof(putDone)) > 0);

    LOG(INFO) << FormatString("Message after put: %s", message_.c_str());
}
}  // namespace st
}  // namespace datasystem
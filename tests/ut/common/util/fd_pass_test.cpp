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
 * Description: fd pass test.
 */
#include "datasystem/common/util/fd_pass.h"
#include "common/util/fd_pass_test.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <securec.h>

#include "ut/common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace ut {
TEST_F(FdPassTest, TestBasicFunction)
{
    LOG(INFO) << "Test fd pass basic function.";
    SocketClient client;
    client.Connect();
    int clientFd = server_->GetClientFd();
    ASSERT_GE(clientFd, 0);

    DS_ASSERT_OK(SockSendFd(clientFd, false, { server_->GetFileFd() }, requestId_));

    std::vector<int> revcFds;
    uint64_t recvRequestId = 0;
    DS_ASSERT_OK(SockRecvFd(client.GetServerFd(), false, revcFds, recvRequestId));

    ASSERT_EQ(recvRequestId, requestId_);
    ASSERT_EQ(revcFds.size(), 1);
    ASSERT_GE(revcFds[0], 0);

    auto *pointer = (uint8_t *)mmap(nullptr, 1024, PROT_READ | PROT_WRITE, MAP_SHARED, revcFds[0], 0);
    ASSERT_NE(pointer, MAP_FAILED);
    std::string msg;
    msg.resize(strlen(SHM_CONTENT));
    ASSERT_EQ(memcpy_s(const_cast<char *>(msg.data()), msg.size(), pointer, msg.size()), EOK);
    ASSERT_EQ(msg, SHM_CONTENT);
    LOG(INFO) << "Test fd pass basic function done.";
}

TEST_F(FdPassTest, TestPassRandomInteger)
{
    LOG(INFO) << "Test fd pass with a random integer (not a fd).";
    int fd = 438;
    int sock1 = -1;
    DS_ASSERT_NOT_OK(SockSendFd(sock1, false, { fd }, requestId_));
    LOG(INFO) << "Test fd pass with a random integer (not a fd) done.";
}

TEST_F(FdPassTest, TestSendFdToDisconnectedClient)
{
    LOG(INFO) << "Test send fd to disconnected client.";

    // 1. Client connect to server, get the server socket fd and disconnect immediately.
    {
        SocketClient client;
        client.Connect();
    }
    usleep(100'000);
    int clientFd = server_->GetClientFd();
    ASSERT_GE(clientFd, 0);

    // 2. Send fd to disconnect client.
    DS_ASSERT_NOT_OK(SockSendFd(clientFd, false, { server_->GetFileFd() }, requestId_));
    LOG(INFO) << "Test send fd to disconnected client done.";
}

TEST_F(FdPassTest, TestSendFdToRandomInteger)
{
    LOG(INFO) << "Test send fd to a random integer (not a socket fd).";

    int clientFd = -1;
    DS_ASSERT_NOT_OK(SockSendFd(clientFd, false, { server_->GetFileFd() }, requestId_));
    LOG(INFO) << "Test send fd to a random integer (not a socket fd) done.";
}

TEST_F(FdPassTest, TestReceiveFdFromRandomInteger)
{
    LOG(INFO) << "Test receive fd from a random integer (not a socket fd).";

    int serverFd = -1;
    std::vector<int> revcFds;
    uint64_t recvRequestId = 0;
    DS_ASSERT_NOT_OK(SockRecvFd(serverFd, false, revcFds, recvRequestId));
    LOG(INFO) << "Test receive fd from a random integer (not a socket fd) done.";
}

TEST_F(FdPassTest, TestReceiveFdFromDumpServer)
{
    LOG(INFO) << "Test receive fd from a dump server.";

    // 1. Connect to server and get server socket fd.
    SocketClient client;
    client.Connect();
    ASSERT_GE(client.GetServerFd(), 0);

    // 2. Shutdown server immediately.
    server_.reset();

    // 3. Receive fd from a dump server.
    std::vector<int> revcFds;
    uint64_t recvRequestId = 0;
    DS_ASSERT_NOT_OK(SockRecvFd(client.GetServerFd(), false, revcFds, recvRequestId));
    LOG(INFO) << "Test receive fd from a dump server done.";
}
}  // namespace ut
}  // namespace datasystem

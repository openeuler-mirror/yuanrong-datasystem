/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: fd pass over scmtcp test.
 */
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/util/fd_pass.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "ut/common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace ut {
constexpr const char *SHM_CONTENT = "Hello, Shared Memory!";
const size_t MMAP_SIZE = 1024;

class ScmTcpSocketClient {
public:
    ScmTcpSocketClient(uint32_t port) : tcpPort_(port)
    {
    }
    ~ScmTcpSocketClient()
    {
        if (serverFd_ > 0) {
            RETRY_ON_EINTR(close(serverFd_));
        }
    }

    void Connect()
    {
        auto endpoint = FormatString("tcp://%s:%d", "127.0.0.1", tcpPort_);
        UnixSockFd sock(RPC_NO_FILE_FD, true);
        DS_ASSERT_OK(sock.Connect(endpoint));
        DS_ASSERT_OK(sock.SetTimeout(STUB_FRONTEND_TIMEOUT));
        serverFd_ = sock.GetFd();
        int waitTimeSec = 2;
        sleep(waitTimeSec);  // wait server accept
    }

    int32_t GetServerFd() const
    {
        return serverFd_;
    }

private:
    std::string sockPath_;
    int32_t serverFd_ = -1;
    uint32_t tcpPort_;
};

class ScmTcpSocketServer {
public:
    ScmTcpSocketServer(uint32_t port) : tcpPort_(port)
    {
    }

    ~ScmTcpSocketServer()
    {
        stop_ = true;
        if (acceptTrd_.joinable()) {
            acceptTrd_.join();
        }
        if (clientFd_ > 0) {
            RETRY_ON_EINTR(close(clientFd_));
        }
        if (listenFd_ > 0) {
            RETRY_ON_EINTR(close(listenFd_));
        }
    }

    void Init()
    {
        // bind socket fd.
        UnixSockFd scmSockFd(RPC_NO_FILE_FD, true);
        std::string tmp;
        auto rc = scmSockFd.Bind(FormatString("tcp://%s:%d", "127.0.0.1", tcpPort_), RPC_SOCK_MODE, tmp);
        if (rc.IsError()) {
            if (errno == EINVAL) {
                GTEST_SKIP() << "SCM TCP not supported on this platform";
                return;
            }
            ASSERT_TRUE(false) << "Failed to bind SCM TCP socket: " << rc.ToString();
        }
        listenFd_ = scmSockFd.GetFd();

        // create shm fd.
        std::string fileTemplate = "/dev/shm/test-XXXXXX";
        std::vector<char> fileName(fileTemplate.begin(), fileTemplate.end());
        fileName.push_back('\0');
        fileFd_ = mkstemp(&fileName[0]);
        ASSERT_GE(fileFd_, 0);
        LOG(INFO) << "File descriptor: " << fileFd_;
        ASSERT_EQ(unlink(&fileName[0]), 0);
        ASSERT_EQ(ftruncate(fileFd_, (off_t)MMAP_SIZE), 0);
        char *space = (char *)mmap(nullptr, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fileFd_, 0);
        ASSERT_EQ(memcpy_s(space, MMAP_SIZE, SHM_CONTENT, strlen(SHM_CONTENT)), EOK);

        // accept client connection.
        acceptTrd_ = std::thread(std::bind(&ScmTcpSocketServer::Start, this));
    }

    int GetClientFd() const
    {
        return clientFd_;
    }

    int GetFileFd() const
    {
        return fileFd_;
    }

private:
    void Start()
    {
        while (!stop_) {
            struct pollfd pfd{ .fd = listenFd_, .events = POLLIN, .revents = 0 };
            int n = poll(&pfd, 1, RPC_POLL_TIME);
            if (n <= 0) {
                continue;
            }
            int fd = accept(listenFd_, nullptr, 0);
            if (fd > 0) {
                clientFd_ = fd;
            }
        }
    }

    std::thread acceptTrd_;
    std::atomic<int> clientFd_ = -1;
    int listenFd_ = -1;
    std::atomic<bool> stop_ = false;
    int fileFd_ = -1;
    uint32_t tcpPort_;
};

class FdPassOverTcpTest : public CommonTest {
public:
    void SetUp() override
    {
        GetFreePort();
        StartServer();
    }

    void TearDown() override
    {
    }

    void StartServer()
    {
        server_ = std::make_unique<ScmTcpSocketServer>(port_);
        server_->Init();
    }

    void GetFreePort()
    {
        int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ASSERT_GE(sockfd, 0);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;

        ASSERT_EQ(bind(sockfd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

        socklen_t addrLen = sizeof(addr);
        ASSERT_EQ(getsockname(sockfd, reinterpret_cast<struct sockaddr *>(&addr), &addrLen), 0);
        port_ = ntohs(addr.sin_port);
        LOG(INFO) << "Free port: " << port_;

        RETRY_ON_EINTR(close(sockfd));
    }

protected:
    std::unique_ptr<ScmTcpSocketServer> server_;
    uint32_t port_ = 0;
    const uint64_t requestId_ = 1;
};

TEST_F(FdPassOverTcpTest, TestBasicFunction)
{
    LOG(INFO) << "Test fd pass over scmtpc basic function.";
    ScmTcpSocketClient client(port_);
    client.Connect();
    int clientFd = server_->GetClientFd();
    ASSERT_GE(clientFd, 0);

    DS_ASSERT_OK(SockSendFd(clientFd, true, { server_->GetFileFd() }, requestId_));

    std::vector<int> revcFds;
    uint64_t recvRequestId = 0;
    DS_ASSERT_OK(SockRecvFd(client.GetServerFd(), true, revcFds, recvRequestId));

    ASSERT_EQ(recvRequestId, requestId_);
    ASSERT_EQ(revcFds.size(), 1);
    ASSERT_GE(revcFds[0], 0);

    auto *pointer = (uint8_t *)mmap(nullptr, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, revcFds[0], 0);
    ASSERT_NE(pointer, MAP_FAILED);
    std::string msg;
    msg.resize(strlen(SHM_CONTENT));
    ASSERT_EQ(memcpy_s(const_cast<char *>(msg.data()), msg.size(), pointer, msg.size()), EOK);
    ASSERT_EQ(msg, SHM_CONTENT);
    LOG(INFO) << "Test fd pass over scmtcp basic function done.";
}

TEST_F(FdPassOverTcpTest, TestPassRandomInteger)
{
    LOG(INFO) << "Test fd pass with a random integer (not a fd).";
    const int fd = 438;
    int sock1 = -1;
    DS_ASSERT_NOT_OK(SockSendFd(sock1, false, { fd }, requestId_));
    LOG(INFO) << "Test fd pass with a random integer (not a fd) done.";
}

TEST_F(FdPassOverTcpTest, TestSendFdToDisconnectedClient)
{
    LOG(INFO) << "Test send fd to disconnected client.";

    // 1. Client connect to server, get the server socket fd and disconnect immediately.
    {
        ScmTcpSocketClient client(port_);
        client.Connect();
    }
    const uint64_t waitTime = 100'000;
    usleep(waitTime);
    int clientFd = server_->GetClientFd();
    ASSERT_GE(clientFd, 0);

    // 2. Send fd to disconnect client.
    DS_ASSERT_NOT_OK(SockSendFd(clientFd, false, { server_->GetFileFd() }, requestId_));
    LOG(INFO) << "Test send fd to disconnected client done.";
}
}  // namespace ut
}  // namespace datasystem
